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


// --- NUOVA LOGICA: GESTIONE TAB SPA & MONITORAGGIO OPERATORE ---

function openTab(tabName) {
    // Nascondi tutti i contenuti
    document.querySelectorAll('.tab-content').forEach(tc => tc.classList.remove('active'));
    // Disattiva tutti i bottoni
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));

    // Attiva la tab richiesta
    const targetTab = document.getElementById(`tab-${tabName}`);
    if (targetTab) targetTab.classList.add('active');

    // Evidenzia il bottone cliccato
    if (window.event && window.event.currentTarget) {
        window.event.currentTarget.classList.add('active');
    }

    // Se passiamo al monitoraggio, aggiorna le card dei sensori
    if (tabName === 'monitoring') {
        refreshMonitoring();
    }
}

// Polling/Fetch per la Dashboard Operatore (Scheda 2)
async function refreshMonitoring() {
    try {
        const res = await fetch('/api/sensors/status'); // Endpoint per avere lista dettagliata + stato FSM
        const sensorList = await res.json();

        const container = document.getElementById('sensor-status-list');
        container.innerHTML = '';

        if (!sensorList || sensorList.length === 0) {
            container.innerHTML = '<p>Nessun sensore disponibile nel sistema.</p>';
            return;
        }

        sensorList.forEach(sensor => {
            // sensor = { id: "S1", state: "READY", machine: "Pressa-1", type: "Vibration" }
            const stateClass = (sensor.state || 'READY').toLowerCase();

            const card = document.createElement('div');
            card.className = `sensor-card state-${stateClass}`;
            card.innerHTML = `
                <div class="sensor-header">
                    <span class="sensor-title">${sensor.id}</span>
                    <span class="status-badge ${stateClass}">${sensor.state}</span>
                </div>
                <div class="sensor-details">
                    <div>Macchina: <b>${sensor.machineToControl || 'N/D'}</b></div>
                    <div>Tipo: <b>${sensor.type || 'N/D'}</b></div>
                </div>
                <div class="sensor-actions">
                    <button class="btn btn-danger" onclick="sendOperatorCommand('${sensor.id}', 'STOP')">STOP</button>
                    <button class="btn btn-warning" onclick="sendOperatorCommand('${sensor.id}', 'RESTART')">RESTART</button>
                </div>
            `;
            container.appendChild(card);
        });
    } catch (err) {
        console.error("Errore durante il caricamento del monitoraggio:", err);
    }
}

// Invio comandi diretti dall'operatore
async function sendOperatorCommand(sensorId, action) {
    try {
        await fetch('/api/operator/command', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ sensorId: sensorId, action: action })
        });
        setTimeout(refreshMonitoring, 300);
    } catch (err) {
        console.error("Errore invio comando operatore:", err);
    }
}

// INIZIALIZZAZIONE
document.addEventListener('DOMContentLoaded', () => {
    loadSensors();
    loadSensorTypes();

    const form = document.getElementById('InsertNewSensor');
    if (form) {
        form.addEventListener('submit', createNewSensor);
    }
});