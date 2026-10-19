const $ = id => document.getElementById(id);
let es = null;

async function api(path, body) {
  const res = await fetch(path, body === undefined ? {} : {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
}

function sys(text, err = false) {
  const d = document.createElement('div');
  d.className = 'sys' + (err ? ' err' : '');
  d.textContent = `[${new Date().toLocaleTimeString()}] ${text}`;
  $('log').prepend(d);
}

function setStatus(connected, brokers) {
  $('dot').classList.toggle('on', connected);
  $('statusText').textContent = connected ? `Conectado a ${brokers}` : 'Desconectado';
  if (brokers) $('brokers').value = brokers;
}

async function refreshTopics(select) {
  try {
    const topics = await api('/api/topics');
    const cur = select || $('topic').value;
    $('topic').innerHTML = topics.length
      ? topics.map(t => `<option value="${t.name}">${t.name} (${t.partitions.length} part.)</option>`).join('')
      : '<option value="">(sin topics)</option>';
    if (topics.some(t => t.name === cur)) $('topic').value = cur;
  } catch (e) { sys('Error listando topics: ' + e.message, true); }
}

$('connectBtn').onclick = async () => {
  $('connectBtn').disabled = true;
  try {
    const r = await api('/api/connect', { brokers: $('brokers').value });
    setStatus(true, r.brokers);
    sys('Conectado a ' + r.brokers);
    await refreshTopics();
  } catch (e) { setStatus(false); sys('Error al conectar: ' + e.message, true); }
  $('connectBtn').disabled = false;
};

$('refreshBtn').onclick = () => refreshTopics();

$('createBtn').onclick = async () => {
  const name = $('newTopic').value.trim();
  if (!name) return;
  try {
    await api('/api/topics', { name, partitions: +$('newParts').value || 1 });
    sys('Topic creado: ' + name);
    $('newTopic').value = '';
    await refreshTopics(name);
  } catch (e) { sys('Error creando topic: ' + e.message, true); }
};

$('sendBtn').onclick = async () => {
  const topic = $('topic').value;
  if (!topic) return sys('Elige un topic primero', true);
  let headers = {};
  try { if ($('headers').value.trim()) headers = JSON.parse($('headers').value); }
  catch { return sys('Headers no es JSON válido', true); }
  try {
    const r = await api('/api/produce', {
      topic, partition: +$('partition').value || 0,
      key: $('key').value || null, value: $('value').value, headers
    });
    sys(`Enviado a ${topic} → offset ${r.offset}`);
  } catch (e) { sys('Error enviando: ' + e.message, true); }
};

function stopConsuming() {
  if (es) { es.close(); es = null; sys('Consumidor detenido'); }
  $('consumeBtn').textContent = 'Escuchar';
}

$('consumeBtn').onclick = () => {
  if (es) return stopConsuming();
  const topic = $('topic').value;
  if (!topic) return sys('Elige un topic primero', true);
  const qs = new URLSearchParams({ topic, partition: $('partition').value || 0, from: $('from').value });
  es = new EventSource('/api/consume?' + qs);
  $('consumeBtn').textContent = 'Detener';
  sys(`Escuchando ${topic} (partición ${qs.get('partition')}, ${$('from').selectedOptions[0].text.toLowerCase()})`);
  es.addEventListener('message', ev => {
    const m = JSON.parse(ev.data);
    let value = m.value ?? '';
    try { value = JSON.stringify(JSON.parse(value), null, 2); } catch {}
    const d = document.createElement('div');
    d.className = 'msg';
    const hdrs = Object.keys(m.headers).length ? ` · headers ${JSON.stringify(m.headers)}` : '';
    d.innerHTML = `<div class="meta"></div><pre></pre>`;
    d.querySelector('.meta').textContent =
      `offset ${m.offset} · key ${m.key ?? '∅'} · ${new Date(m.timestamp).toLocaleTimeString()}${hdrs}`;
    d.querySelector('pre').textContent = value;
    $('log').prepend(d);
  });
  es.addEventListener('kafka-error', ev => sys('Kafka: ' + ev.data, true));
  es.onerror = () => { if (es && es.readyState === EventSource.CLOSED) { sys('Conexión SSE cerrada', true); stopConsuming(); } };
};

$('clearBtn').onclick = () => { $('log').innerHTML = ''; };

api('/api/status').then(s => { setStatus(s.connected, s.brokers); if (s.connected) refreshTopics(); });
