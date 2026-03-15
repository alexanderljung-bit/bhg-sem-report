'use client';
import { useState, useEffect, useContext } from 'react';
import { DateContext, DeepDiveContext } from './layout';
import { getHierarchy, getDeepDive, KpiData, SegmentRow, WeeklyRow, DailyRow, CumulativeRow } from '@/lib/api';
import KpiCard from '@/components/KpiCard';
import YoyBadge from '@/components/YoyBadge';
import { fmtNum, yoyPct } from '@/lib/format';
import {
    LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
    BarChart, Bar, Legend, ReferenceLine
} from 'recharts';

export default function DeepDivePage() {
    const { startDate, endDate } = useContext(DateContext);
    const { company, site, setCompany, setSite } = useContext(DeepDiveContext);
    const [companies, setCompanies] = useState<string[]>([]);
    const [sites, setSites] = useState<Record<string, string[]>>({});
    const [loading, setLoading] = useState(false);
    const [data, setData] = useState<{
        kpi: KpiData; segments: SegmentRow[]; weekly: WeeklyRow[];
        daily: DailyRow[]; cumulative: CumulativeRow[];
    } | null>(null);

    // Load hierarchy
    useEffect(() => {
        getHierarchy().then(h => {
            setCompanies(h.companies);
            const siteMap: Record<string, string[]> = {};
            for (const [, companyMap] of Object.entries(h.hierarchy)) {
                for (const [co, siteList] of Object.entries(companyMap)) {
                    siteMap[co] = siteList;
                }
            }
            setSites(siteMap);
        });
    }, []);

    // Load data when filters + dates change
    useEffect(() => {
        if (!startDate || !endDate || !company || !site) return;
        setLoading(true);
        getDeepDive(startDate, endDate, company, site)
            .then(setData)
            .catch(console.error)
            .finally(() => setLoading(false));
    }, [startDate, endDate, company, site]);

    const availableSites = company ? (sites[company] || []) : [];

    return (
        <div>
            {/* Filter bar */}
            <div className="filter-wrapper">
                <div className="filter-bar" style={{ marginBottom: 0 }}>
                    <select value={company} onChange={e => { setCompany(e.target.value); setSite(''); }}>
                        <option value="">— Välj bolag —</option>
                        {companies.map(c => <option key={c} value={c}>{c}</option>)}
                    </select>
                    <select value={site} onChange={e => setSite(e.target.value)} disabled={!company}>
                        <option value="">— Välj sajt —</option>
                        {availableSites.map(s => <option key={s} value={s}>{s}</option>)}
                    </select>
                </div>
                {startDate && endDate && (
                    <span className="filter-date">
                        {startDate} – {endDate}
                    </span>
                )}
            </div>

            {/* Empty state */}
            {(!company || !site) && (
                <div className="empty-state">
                    <svg className="empty-state-icon" style={{ color: 'var(--bhg-blue)' }} width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M3 3v18h18"></path><path d="M18 17V9"></path><path d="M13 17V5"></path><path d="M8 17v-3"></path></svg>
                    <div className="empty-state-title">Välj bolag och sajt</div>
                    <div className="empty-state-text">
                        Välj ett bolag och en sajt ovan för att visa rapporten.<br />
                        För en samlad översikt, använd <strong>Group</strong>-fliken.
                    </div>
                </div>
            )}

            {/* Loading */}
            {loading && (
                <div className="loading">
                    <div className="spinner" />
                    Laddar rapportdata...
                </div>
            )}

            {/* Content */}
            {data && !loading && (
                <>
                    {/* KPI Scorecards */}
                    <div className="kpi-grid">
                        <KpiCard title="Clicks" value={data.kpi.clicks} yoyValue={data.kpi.yoy_clicks} icon="🖱️" />
                        <KpiCard title="Revenue" value={data.kpi.revenue} yoyValue={data.kpi.yoy_revenue} icon="💰" suffix=" SEK" />
                        <KpiCard title="Cost" value={data.kpi.cost} yoyValue={data.kpi.yoy_cost} icon="📊" suffix=" SEK" />
                        <KpiCard title="CoS" value={data.kpi.cos} yoyValue={data.kpi.yoy_cos} icon="📈" suffix="%" invertYoy />
                        <KpiCard title="Transactions" value={data.kpi.transactions} yoyValue={data.kpi.yoy_transactions} icon="🛒" />
                        <KpiCard
                            title="AOV"
                            value={data.kpi.transactions ? Math.round(data.kpi.revenue / data.kpi.transactions) : 0}
                            yoyValue={data.kpi.yoy_transactions ? Math.round(data.kpi.yoy_revenue / data.kpi.yoy_transactions) : 0}
                            icon="💳" suffix=" SEK"
                        />
                    </div>

                    {/* Charts: Left = Cumulative CoS % (line), Right = Daily CoS % (bar) */}
                    <div className="chart-grid">
                        {/* Left: Cumulative CoS % line chart */}
                        <div className="chart-card">
                            <div className="chart-card-title">Cumulative CoS %</div>
                            <ResponsiveContainer width="100%" height={250}>
                                <LineChart data={data.cumulative}>
                                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border-light)" />
                                    <XAxis dataKey="Date" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} tickMargin={8} axisLine={false} tickLine={false} />
                                    <YAxis tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} domain={['auto', 'auto']} unit="%" />
                                    <Tooltip
                                        formatter={(val: any) => [`${Number(val).toFixed(1)}%`, '']}
                                        labelStyle={{ color: 'var(--text-primary)', fontWeight: 600 }}
                                        contentStyle={{ borderRadius: 8, border: 'none', boxShadow: 'var(--shadow-md)', padding: '12px 16px' }}
                                    />
                                    <Legend wrapperStyle={{ paddingTop: 16, fontSize: 12 }} />
                                    <Line type="monotone" dataKey="Cumulative CoS %" name="CoS %" stroke="var(--bhg-blue)" strokeWidth={2.5} dot={false} activeDot={{ r: 5 }} />
                                    <Line type="monotone" dataKey="Budget Target %" name="Budget" stroke="var(--rose-500)" strokeWidth={1.5} strokeDasharray="6 3" dot={false} />
                                </LineChart>
                            </ResponsiveContainer>
                        </div>

                        {/* Right: Daily CoS % bar chart */}
                        <div className="chart-card">
                            <div className="chart-card-title">Daily CoS %</div>
                            <ResponsiveContainer width="100%" height={250}>
                                <BarChart data={data.daily}>
                                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border-light)" />
                                    <XAxis dataKey="Date" tick={{ fontSize: 11, fill: 'var(--text-muted)' }} tickMargin={8} axisLine={false} tickLine={false} />
                                    <YAxis tick={{ fontSize: 11, fill: 'var(--text-muted)' }} axisLine={false} tickLine={false} domain={[0, 'auto']} unit="%" />
                                    <Tooltip
                                        formatter={(val: any) => [`${Number(val).toFixed(1)}%`, '']}
                                        labelStyle={{ color: 'var(--text-primary)', fontWeight: 600 }}
                                        contentStyle={{ borderRadius: 8, border: 'none', boxShadow: 'var(--shadow-md)', padding: '12px 16px' }}
                                    />
                                    <Bar dataKey="CoS %" name="CoS %" fill="var(--bhg-blue)" radius={[4, 4, 0, 0]} />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </div>

                    {/* Segment Table */}
                    {data.segments.length > 0 && (
                        <div className="data-table-card">
                            <div className="data-table-header">
                                <span className="data-table-title">Performance by Segment</span>
                            </div>
                            <div className="table-responsive">
                                <table className="data-table">
                                <thead>
                                    <tr>
                                        <th>Segment</th><th>Clicks</th><th>Transactions</th><th>CR %</th>
                                        <th>Revenue</th><th>Cost</th><th>CoS</th><th>YoY</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {data.segments.map(s => (
                                        <tr key={s.Segment}>
                                            <td style={{ fontWeight: 600 }}>{s.Segment}</td>
                                            <td>{s.Clicks.toLocaleString()}</td>
                                            <td>{s.Transactions.toLocaleString()}</td>
                                            <td>{s['CR %'].toFixed(1)}%</td>
                                            <td className="td-bold">{fmtNum(s['Revenue (SEK)'])} SEK</td>
                                            <td>{fmtNum(s['Cost (SEK)'])} SEK</td>
                                            <td className="td-bold">{s['CoS %'].toFixed(1)}%</td>
                                            <td><YoyBadge value={s['Revenue YoY %']} /></td>
                                        </tr>
                                    ))}
                                    {(() => {
                                        const totClicks = data.segments.reduce((a, s) => a + s.Clicks, 0);
                                        const totTxn = data.segments.reduce((a, s) => a + s.Transactions, 0);
                                        const totRev = data.segments.reduce((a, s) => a + s['Revenue (SEK)'], 0);
                                        const totCost = data.segments.reduce((a, s) => a + s['Cost (SEK)'], 0);
                                        const totCr = totClicks ? (totTxn / totClicks * 100) : 0;
                                        const totCos = totRev ? (totCost / totRev * 100) : 0;
                                        const totYoy = data.kpi.yoy_revenue ? ((totRev / data.kpi.yoy_revenue - 1) * 100) : 0;
                                        return (
                                            <tr style={{ borderTop: '2px solid var(--border)' }}>
                                                <td style={{ fontWeight: 800 }}>Total</td>
                                                <td className="td-bold">{totClicks.toLocaleString()}</td>
                                                <td className="td-bold">{totTxn.toLocaleString()}</td>
                                                <td className="td-bold">{totCr.toFixed(1)}%</td>
                                                <td className="td-bold">{fmtNum(totRev)} SEK</td>
                                                <td className="td-bold">{fmtNum(totCost)} SEK</td>
                                                <td className="td-bold">{totCos.toFixed(1)}%</td>
                                                <td><YoyBadge value={totYoy} /></td>
                                            </tr>
                                        );
                                    })()}
                                </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* Weekly Table */}
                    {data.weekly.length > 0 && (
                        <div className="data-table-card">
                            <div className="data-table-header">
                                <span className="data-table-title">Weekly Performance</span>
                            </div>
                            <div className="table-responsive">
                                <table className="data-table">
                                <thead>
                                    <tr>
                                        <th>Week</th><th>Clicks</th><th>Revenue</th><th>Cost</th><th>CoS</th><th>YoY</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {data.weekly.map(w => (
                                        <tr key={w.Week}>
                                            <td style={{ fontWeight: 500 }}>{w.Week}</td>
                                            <td>{w.Clicks.toLocaleString()}</td>
                                            <td className="td-bold">{fmtNum(w['Revenue (SEK)'])} SEK</td>
                                            <td>{fmtNum(w['Cost (SEK)'])} SEK</td>
                                            <td className="td-bold">{w['CoS %'].toFixed(1)}%</td>
                                            <td><YoyBadge value={w['Revenue YoY %']} /></td>
                                        </tr>
                                    ))}
                                </tbody>
                                </table>
                            </div>
                        </div>
                    )}
                </>
            )}
        </div>
    );
}
