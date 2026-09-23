// --- LE TUE FUNZIONI ESISTENTI (MANTENUTE INTATTE) ---

async function loadSensors() {
    try {
        const res = await fetch('/api/sensors');
        const sensors = await res.json();
        const select = document.getElementById('sensorSelect');
        select.innerHTML = '';

        if (!sensors || sensors.length === 0) {
            select.innerHTML = '<option value="">Nessun sensore attivo</option>';
            return;
        }

        // Estraiamo array dalla chiave "active_sensors"
        if (sensors && Array.isArray(sensors.active_sensors)) {
            sensors.active_sensors.forEach(t => {
                const opt = document.createElement('option');
                opt.value = t;
                opt.textContent = t;
                select.appendChild(opt);
            });
        }
    } catch (err) {
        console.error(err);
        document.getElementById('log').textContent = "Errore caricamento sensori";
    }
}

async function sendCommand(mode) {
    const select = document.getElementById('sensorSelect');
    const sensorID = select.value;
    const val = parseFloat(document.getElementById('magnitudeInput').value) || 0;

    if (!sensorID) {
        alert("Seleziona prima un sensore valido!");
        return;
    }

    const payload = {
        sensorId: sensorID,
        mode: mode,
        spikeMagnitude: mode === 'ModeSpike' ? val : 0,
        driftRate: mode === 'ModeDrift' ? val : 0
    };

    try {
        const res = await fetch('/api/command', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const text = await res.text();
        document.getElementById('log').textContent = `[${new Date().toLocaleTimeString()}] ${text}`;

        if (mode === 'ModeStop' && res.ok) {
            setTimeout(() => { loadSensors(); }, 1000);
        }
    } catch (err) {
        document.getElementById('log').textContent = `[${new Date().toLocaleTimeString()}] Errore di connessione`;
    }
}

async function createNewSensor(event) {
    if (event) event.preventDefault();

    const rateSeconds = parseInt(document.getElementById('interval').value) || 1;

    const payload = {
        sensorId: document.getElementById('sensorId').value,
        type: document.getElementById('newSensorSelect').value,
        machineToControl: document.getElementById('machineToControl').value,
        baseMean: parseFloat(document.getElementById('baseMean').value),
        variance: parseFloat(document.getElementById('variance').value),
        interval: rateSeconds,
        soglia_minima: parseFloat(document.getElementById('soglia_minima').value),
        soglia_massima: parseFloat(document.getElementById('soglia_massima').value),
        max_std_dev: parseFloat(document.getElementById('max_std_dev').value),
        max_drift: parseFloat(document.getElementById('max_drift').value),
    };

    try {
        const res = await fetch('/api/sensors', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const text = await res.text();
        document.getElementById('logInsert').textContent = `[${new Date().toLocaleTimeString()}] ${text}`;

        if (res.ok) {
            document.getElementById('InsertNewSensor').reset();
            setTimeout(() => { loadSensors(); refreshMonitoring(); }, 500);
        }
    } catch (err) {
        console.error("Errore fetch:", err);
        document.getElementById('logInsert').textContent = `[${new Date().toLocaleTimeString()}] Errore durante l'inserimento`;
    }
}

function resetInsertLog() {
    document.getElementById('logInsert').textContent = "Stato: Form resettato.";
}

async function loadSensorTypes() {
    try {
        const res = await fetch('/api/sensor-types');
        const data = await res.json();
        const select = document.getElementById('newSensorSelect');

        select.innerHTML = '<option value="">Scegli tipo di sensore</option>';

        // Estraiamo array dalla chiave "sensor_types"
        if (data && Array.isArray(data.sensor_types)) {
            data.sensor_types.forEach(t => {
                const opt = document.createElement('option');
                opt.value = t;
                opt.textContent = t;
                select.appendChild(opt);
            });
        }
    } catch (err) {
        console.error("Errore nel caricamento dei tipi di sensore:", err);
    }
}


// --- GESTIONE TAB SPA & MONITORAGGIO ---

function openTab(tabName, evt) {
    // Nascondi tutti i contenuti
    document.querySelectorAll('.tab-content').forEach(tc => tc.classList.remove('active'));
    // Disattiva tutti i bottoni
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));

    // Attiva la tab richiesta
    const targetTab = document.getElementById(`tab-${tabName}`) || document.getElementById(tabName);
    if (targetTab) {
        targetTab.classList.add('active');
    }

    // Evidenzia il bottone cliccato
    if (evt && evt.currentTarget) {
        evt.currentTarget.classList.add('active');
    } else {
        // Fallback: cerca il bottone associato se chiamato da codice
        const btn = document.querySelector(`.tab-btn[onclick*="${tabName}"]`);
        if (btn) btn.classList.add('active');
    }

    // Se entriamo nella tab di monitoraggio, eseguiamo subito il fetch delle metriche
    if (tabName === 'monitoring') {
        refreshMetrics();
    }
}

// --- FETCH & POLLING METRICHE ---

async function refreshMetrics() {
    try {
        const res = await fetch('/api/monitoring/sensor');
        const data = await res.json();

        // LOG DI DEBUG
        console.log("[DEBUG Metrics Data]:", data);

        const tbody = document.getElementById('metrics-table-body');
        if (!tbody) return;

        if (data.status !== 'SUCCESS' || !data.metrics || data.metrics.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" class="empty-text">Nessuna metrica disponibile al momento.</td></tr>';
            return;
        }

        tbody.innerHTML = ''; // Pulizia tabella

        data.metrics.forEach(metric => {
            // Estrazione sicura dei campi gestendo sia camelCase che snake_case (e controllando 'undefined')
            const stdDevValue = metric.stdDev !== undefined ? metric.stdDev : metric.std_dev;
            const rateOfChangeValue = metric.rateOfChange !== undefined ? metric.rateOfChange : metric.rate_of_change;

            const row = document.createElement('tr');
            row.innerHTML = `
                <td><b>${metric.sensorId || metric.sensor_id || 'N/D'}</b></td>
                <td>${metric.machineId || metric.machine_id || 'N/D'}</td>
                <td>${formatNumber(metric.media)}</td>
                <td>${formatNumber(metric.minimo)}</td>
                <td>${formatNumber(metric.massimo)}</td>
                <td>${formatNumber(stdDevValue)}</td>
                <td>${formatNumber(rateOfChangeValue)}</td>
            `;
            tbody.appendChild(row);
        });
    } catch (err) {
        console.error("Errore durante il recupero delle metriche:", err);
    }
}

// Utility per formattare i decimali a 2 cifre
function formatNumber(val) {
    if (val === undefined || val === null || isNaN(val)) return '-';
    return Number(val).toFixed(8);
}

// --- INIZIALIZZAZIONE UNIFICATA ---

document.addEventListener('DOMContentLoaded', () => {
    // Carica i dati iniziali dei form/selezioni
    if (typeof loadSensors === 'function') loadSensors();
    if (typeof loadSensorTypes === 'function') loadSensorTypes();

    // Registra l'evento di invio del form
    const form = document.getElementById('InsertNewSensor');
    if (form && typeof createNewSensor === 'function') {
        form.addEventListener('submit', createNewSensor);
    }

    // Ticker periodico in background per le metriche real-time
    setInterval(() => {
        // Controlla se la tab è attiva sia tramite classe 'active' sia tramite visibilità CSS
        const monitoringTab = document.getElementById('tab-monitoring') || document.getElementById('monitoring');

        if (monitoringTab) {
            const isActive = monitoringTab.classList.contains('active');
            const isVisible = window.getComputedStyle(monitoringTab).display !== 'none';

            if (isActive || isVisible) {
                refreshMetrics();
            }
        }
    }, 60 * 1000);
});

document.addEventListener('DOMContentLoaded', () => {
    // Apri la connessione SSE verso lo endpoint alimentato da Redis
    const eventSource = new EventSource('/api/monitoring/sensor/stream');

    eventSource.addEventListener('sensor_data', (event) => {
        // Parsa il JSON inviato da Redis
        const sensorMetric = JSON.parse(event.data);

        // Aggiorna o crea la Card nel DOM
        updateOrCreateSensorCard(sensorMetric);
    });

    eventSource.onerror = (err) => {
        console.error("Errore nella connessione Pub/Sub Real-Time:", err);
    };
});

/**
 * Funzione per creare o aggiornare la Card sintetica del sensore
 */
function updateOrCreateSensorCard(sensorData) {
    const grid = document.getElementById('sensors-grid');
    if (!grid) return;

    const sensorId = sensorData.sensorId || 'N/D';
    const machineId = sensorData.machineId || 'N/D';
    const rawStatus = (sensorData.status || 'READY').toUpperCase();

    // Mappatura classe CSS e colore in base ai 3 stati
    let statusClass = 'status-ready';
    let statusLabel = 'READY';

    if (rawStatus === 'STOPPED') {
        statusClass = 'status-stopped';
        statusLabel = 'STOPPED';
    } else if (rawStatus === 'WARM-UP' || rawStatus === 'WARM_UP') {
        statusClass = 'status-warmup';
        statusLabel = 'WARM-UP';
    }

    // Cerca se la card esiste già
    let card = document.getElementById(`card-${sensorId}`);

    if (!card) {
        card = document.createElement('div');
        card.id = `card-${sensorId}`;
        grid.appendChild(card);
    }

    // Assegna la classe base e quella relativa allo stato corrente
    card.className = `sensor-card ${statusClass}`;

    // Contenuto minimale della Card
    card.innerHTML = `
        <div class="sensor-header">
            <div>
                <div class="sensor-title">${sensorId}</div>
                <small style="color: #64748b;">Macchina: <b>${machineId}</b></small>
            </div>
            <span class="status-badge ${statusClass}">${statusLabel}</span>
        </div>
    `;
}