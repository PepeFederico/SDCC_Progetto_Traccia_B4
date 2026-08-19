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

        sensors.forEach(s => {
            const opt = document.createElement('option');
            opt.value = s;
            opt.textContent = s;
            select.appendChild(opt);
        });
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

    const rateSeconds = parseInt(document.getElementById('rate').value) || 1;

    const payload = {
        sensorId: document.getElementById('sensorId').value,
        type: document.getElementById('newSensorSelect').value,
        machineToControl: document.getElementById('machineToControl').value,
        baseMean: parseFloat(document.getElementById('mean').value),
        variance: parseFloat(document.getElementById('variance').value),
        interval: rateSeconds * 1000000000 // Nanosecondi per Go time.Duration
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
            setTimeout(() => { loadSensors(); }, 500);
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
        const types = await res.json();
        const select = document.getElementById('newSensorSelect');

        select.innerHTML = '<option value="">Scegli tipo di sensore</option>';

        if (types) {
            types.forEach(t => {
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

// Inizializzazione
document.addEventListener('DOMContentLoaded', () => {
    loadSensors();
    loadSensorTypes();

    const form = document.getElementById('InsertNewSensor');
    if (form) {
        form.addEventListener('submit', createNewSensor);
    }
});