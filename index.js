const { BigQuery } = require('@google-cloud/bigquery');
const axios = require('axios');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, GetCommand, PutCommand } = require('@aws-sdk/lib-dynamodb');

// ============================================================================
// 🛠️ CONFIGURACIÓN Y CLIENTES NUBE
// ============================================================================

const delay = ms => new Promise(res => setTimeout(res, ms));

const POWERCONTROL_BASE_URL = 'https://lo97hgr3l2.execute-api.us-east-1.amazonaws.com/prod/api/v1/events';
const POWERCONTROL_MANUAL_ALERT_URL = 'https://lo97hgr3l2.execute-api.us-east-1.amazonaws.com/prod/api/v1/events/manual-alert';
const API_KEY = process.env.POWERCONTROL_API_KEY;

// Inicialización segura de BigQuery usando las variables inyectadas por Serverless
const bigquery = new BigQuery({
    projectId: process.env.GCP_PROJECT_ID,
    credentials: {
        client_email: process.env.GCP_CLIENT_EMAIL,
        // CRÍTICO: Reconstruir los saltos de línea de la llave privada
        private_key: process.env.GCP_PRIVATE_KEY ? process.env.GCP_PRIVATE_KEY.replace(/\\n/g, '\n') : ''
    }
});

const ddbClient = new DynamoDBClient({ region: 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(ddbClient);

// ============================================================================
// 🔖 MEMORIA PERSISTENTE (DYNAMODB)
// ============================================================================

async function getLastSyncDate() {
    const params = {
        TableName: 'PowerControl_State',
        Key: { id: 'LAST_SYNC' }
    };
    try {
        const { Item } = await docClient.send(new GetCommand(params));
        if (Item && Item.lastSync) return Item.lastSync;
    } catch (e) {
        console.warn('⚠️ No se encontró estado previo en DynamoDB. Usando fallback de 2 horas.');
    }
    const fechaInicial = new Date(Date.now() - 2 * 60 * 60 * 1000);
    return fechaInicial.toISOString().replace('T', ' ').substring(0, 19);
}

async function saveLastSyncDate(newDate) {
    await docClient.send(new PutCommand({
        TableName: 'PowerControl_State',
        Item: { id: 'LAST_SYNC', lastSync: newDate }
    }));
}

async function isMerchantAlerted(merchantId) {
    const { Item } = await docClient.send(new GetCommand({
        TableName: 'PowerControl_AlertedMerchants',
        Key: { merchant_id: String(merchantId) }
    }));
    return !!Item;
}

async function saveAlertedMerchant(merchantId) {
    await docClient.send(new PutCommand({
        TableName: 'PowerControl_AlertedMerchants',
        Item: { merchant_id: String(merchantId), alerted_at: new Date().toISOString() }
    }));
}

// ============================================================================
// 📦 NÚCLEO 1: EXTRACCIÓN TRANSACCIONAL (RT/NRT)
// ============================================================================

async function processNewEvents() {
    console.log('⏳ [AGENTE] Iniciando ingesta desde BigQuery...');
    const lastSync = await getLastSyncDate();
    let maxDateProcessed = lastSync;

    const query = `
        SELECT JSON_EXTRACT(extra_data, '$.request') AS payload_request, fecha, servicio
        FROM \`bbva-bnpl-pe-mo-project.logs_bbva_bnpl_pe.logs_events_bbva\`
        WHERE servicio IN ('FullApplicationRT', 'FullApplicationNRT')
        AND CAST(fecha AS TIMESTAMP) > CAST(@lastSync AS TIMESTAMP)
        AND JSON_EXTRACT(extra_data, '$.request') IS NOT NULL
        ORDER BY CAST(fecha AS TIMESTAMP) ASC
        LIMIT 1000
    `;

    const [rows] = await bigquery.query({ query, params: { lastSync } });

    if (rows.length === 0) {
        console.log('✅ [AGENTE] Cero eventos nuevos.');
        return 'SUCCESS_NO_DATA';
    }

    console.log(`📦 Procesando ${rows.length} eventos hacia PowerControl AWS...`);

    for (const row of rows) {
        const payload = typeof row.payload_request === 'string' ? JSON.parse(row.payload_request) : row.payload_request;
        const targetUrl = `${POWERCONTROL_BASE_URL}/${row.servicio.charAt(0).toLowerCase() + row.servicio.slice(1)}`;

        try {
            await axios.post(targetUrl, payload, { headers: { 'x-api-key': API_KEY } });
            console.log(`  ✔️ Evento enrutado [${row.servicio}]: ${payload.applicationid}`);
            
            maxDateProcessed = new Date(row.fecha.value || row.fecha).toISOString();
            await saveLastSyncDate(maxDateProcessed);
            await delay(150); // Evasión Firewall

        } catch (error) {
            console.error(`  ❌ Error API PowerControl: ${error.message}`);
            break;
        }
    }
    return 'SUCCESS_PROCESSED';
}

// ============================================================================
// 🚨 NÚCLEO 2: ALERTAS DE RIESGO DIARIAS
// ============================================================================

async function processDailyAlerts() {
    console.log('🚨 [ALERTAS] Buscando comercios con riesgo de impago alto...');
    const query = `
        SELECT pl.merchant_id AS customer_id, m.display_name, COUNT(*) AS total_creditos,
               COUNTIF(c.status = 'disabled') AS creditos_disabled,
               ROUND(COUNTIF(c.status = 'disabled') * 100.0 / COUNT(*), 2) AS porcentaje_impagos
        FROM \`bbva-bnpl-pe-mo-project.rds_bbva.loan\` l 
        INNER JOIN \`bbva-bnpl-pe-mo-project.rds_bbva.pre_loan\` pl ON pl.id = l.pre_loan_id 
        INNER JOIN \`bbva-bnpl-pe-mo-project.rds_bbva.customer\` c ON c.id = pl.customer_id
        INNER JOIN \`bbva-bnpl-pe-mo-project.rds_bbva.merchant\` m ON pl.merchant_id = m.id  
        WHERE l.created_at <= TIMESTAMP(DATETIME_SUB(CURRENT_DATETIME('America/Lima'), INTERVAL 1 MONTH))
        GROUP BY pl.merchant_id, m.display_name
        HAVING ROUND(COUNTIF(c.status = 'disabled') * 100.0 / COUNT(*), 2) > 80 AND COUNTIF(c.status = 'disabled') > 5
    `;

    const [rows] = await bigquery.query({ query });

    for (const row of rows) {
        const yaAlertado = await isMerchantAlerted(row.customer_id);
        if (yaAlertado) continue;

        const payload = {
            customer_id: row.customer_id, 
            application_id: null, 
            review_comment: `COMERCIO EN RIESGO: "${row.display_name}" - ${row.porcentaje_impagos}% de impagos (${row.creditos_disabled} vencidos de ${row.total_creditos} totales).`,
            severity: "HIGH"
        };

        try {
            await axios.post(POWERCONTROL_MANUAL_ALERT_URL, payload, { headers: { 'x-api-key': API_KEY } });
            console.log(`  ✔️ Alerta creada para ${row.display_name}`);
            await saveAlertedMerchant(row.customer_id);
            await delay(150);
        } catch (err) {
            console.error(`  ❌ Fallo en alerta para ${row.display_name}:`, err.message);
        }
    }
    return 'ALERTS_COMPLETED';
}

// ============================================================================
// 🚀 HANDLER PRINCIPAL AWS LAMBDA
// ============================================================================

exports.handler = async (event) => {
    console.log('⚡ Agente PowerControl Invocado. Tarea:', event.taskType || 'DESCONOCIDA');

    // Enrutador inteligente basado en el input de AWS EventBridge
    if (event.taskType === 'DAILY_ALERTS') {
        await processDailyAlerts();
    } else {
        await processNewEvents();
    }

    return { statusCode: 200, body: 'Ejecución finalizada.' };
};