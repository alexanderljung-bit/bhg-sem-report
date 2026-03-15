'use client';
import { useState, useEffect } from 'react';
import { getSources, addSource, deleteSource, testConnection, Source } from '@/lib/api';

export default function SourcesPage() {
    const [sources, setSources] = useState<Source[]>([]);
    const [businessAreas, setBusinessAreas] = useState<string[]>([]);
    const [companyList, setCompanyList] = useState<string[]>([]);
    const [form, setForm] = useState({
        dataset_id: '', label: '', business_area: '', company: '',
        vat_status: 'ex_vat', gads_customer_id: '',
    });
    const [testResult, setTestResult] = useState<string | null>(null);
    const [message, setMessage] = useState('');

    const loadSources = () => {
        getSources().then(d => {
            setSources(d.sources);
            setBusinessAreas(d.business_areas);
            setCompanyList(d.companies);
        });
    };

    useEffect(() => { loadSources(); }, []);

    const handleAdd = async () => {
        try {
            await addSource(form);
            setForm({
                dataset_id: '', label: '', business_area: '', company: '',
                vat_status: 'ex_vat', gads_customer_id: ''
            });
            setMessage('✅ Source added successfully!');
            loadSources();
        } catch (err: unknown) {
            setMessage(`❌ ${err instanceof Error ? err.message : 'Error'}`);
        }
    };

    const handleDelete = async (id: string) => {
        if (!confirm(`Remove ${id}?`)) return;
        await deleteSource(id);
        loadSources();
    };

    const handleTest = async () => {
        if (!form.dataset_id) return;
        try {
            const r = await testConnection(form.dataset_id);
            setTestResult(`✅ ${r.table_count} tables found (${r.first_date} → ${r.last_date})`);
        } catch (err: unknown) {
            setTestResult(`❌ ${err instanceof Error ? err.message : 'Error'}`);
        }
    };

    return (
        <div>
            {/* Add New Source */}
            <h2 className="section-title">➕ Add New Source</h2>
            {message && <div className="info-banner"><span>{message}</span></div>}

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
                <div className="form-group">
                    <label className="form-label">GA4 Property ID</label>
                    <input type="text" placeholder="e.g. 269613104" value={form.dataset_id.replace('analytics_', '')}
                        onChange={e => setForm({ ...form, dataset_id: `analytics_${e.target.value}` })} />
                </div>
                <div className="form-group">
                    <label className="form-label">Label</label>
                    <input type="text" placeholder="e.g. Bygghemma.se" value={form.label}
                        onChange={e => setForm({ ...form, label: e.target.value })} />
                </div>
                <div className="form-group">
                    <label className="form-label">Business Area</label>
                    <select value={form.business_area} onChange={e => setForm({ ...form, business_area: e.target.value })}>
                        <option value="">— Select —</option>
                        {businessAreas.map(ba => <option key={ba} value={ba}>{ba}</option>)}
                    </select>
                </div>
                <div className="form-group">
                    <label className="form-label">Company</label>
                    <select value={form.company} onChange={e => setForm({ ...form, company: e.target.value })}>
                        <option value="">— Select —</option>
                        {companyList.map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                </div>
                <div className="form-group">
                    <label className="form-label">Google Ads Customer ID</label>
                    <input type="text" placeholder="e.g. 9855672833" value={form.gads_customer_id}
                        onChange={e => setForm({ ...form, gads_customer_id: e.target.value })} />
                </div>
                <div className="form-group">
                    <label className="form-label">VAT Status</label>
                    <select value={form.vat_status} onChange={e => setForm({ ...form, vat_status: e.target.value })}>
                        <option value="ex_vat">Excl. VAT</option>
                        <option value="inc_vat">Incl. VAT</option>
                    </select>
                </div>
            </div>

            <div style={{ display: 'flex', gap: 12, marginBottom: 24 }}>
                <button className="btn" onClick={handleTest}>🔍 Test Connection</button>
                <button className="btn btn-primary" onClick={handleAdd}>⚡ Connect</button>
            </div>
            {testResult && <div className="info-banner"><span>{testResult}</span></div>}

            {/* Connected Sources */}
            <h2 className="section-title" style={{ marginTop: 32 }}>🔗 Connected Sources ({sources.length})</h2>
            {sources.map(s => (
                <div key={s.dataset_id} className="source-card">
                    <div className="source-card-info">
                        <span className="source-card-label">{s.label}</span>
                        <span className="source-card-meta">
                            {s.company} · {s.business_area} · GA4: {s.ga4_property_id}
                            {s.gads_customer_id && ` · GAds: ${s.gads_customer_id}`}
                        </span>
                    </div>
                    <button className="btn btn-sm" onClick={() => handleDelete(s.dataset_id)}
                        style={{ color: 'var(--rose-600)', borderColor: 'var(--rose-600)' }}>
                        🗑️ Remove
                    </button>
                </div>
            ))}
        </div>
    );
}
