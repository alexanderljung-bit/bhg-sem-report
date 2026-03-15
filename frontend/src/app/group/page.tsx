'use client';
import React, { useState, useEffect, useContext } from 'react';
import { DateContext } from '../layout';
import { getPortfolio, PortfolioRow } from '@/lib/api';
import PerfBar from '@/components/PerfBar';
import { fmtNum } from '@/lib/format';

export default function GroupPage() {
    const { startDate, endDate } = useContext(DateContext);
    const [rows, setRows] = useState<PortfolioRow[]>([]);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        if (!startDate || !endDate) return;
        setLoading(true);
        getPortfolio(startDate, endDate)
            .then(d => setRows(d.rows))
            .catch(console.error)
            .finally(() => setLoading(false));
    }, [startDate, endDate]);

    if (loading) {
        return <div className="loading"><div className="spinner" />Laddar BHG Group Overview...</div>;
    }

    // Group by Business Area > Company > Site
    const areas = new Map<string, Map<string, PortfolioRow[]>>();
    for (const r of rows) {
        if (!areas.has(r['Business Area'])) areas.set(r['Business Area'], new Map());
        const companies = areas.get(r['Business Area'])!;
        if (!companies.has(r.Company)) companies.set(r.Company, []);
        companies.get(r.Company)!.push(r);
    }

    return (
        <div>
            <div className="info-banner">
                <span className="info-banner-icon">ℹ️</span>
                <div>
                    <div className="info-banner-title">BHG Group Overview</div>
                    <div className="info-banner-text">
                        {startDate && endDate ? `${startDate} – ${endDate}` : 'Välj period ovan'}
                    </div>
                </div>
            </div>

            <div className="data-table-card">
                <table className="data-table">
                    <thead>
                        <tr>
                            <th style={{ textAlign: 'left', width: '25%' }}>Business Area / Company / Site</th>
                            <th>Clicks</th><th>Revenue</th><th>Cost</th><th>CoS</th><th>Target</th><th>YoY Growth</th>
                        </tr>
                    </thead>
                    <tbody>
                        {Array.from(areas.entries()).map(([area, companies]) => {
                            const areaRows = rows.filter(r => r['Business Area'] === area);
                            const areaRev = areaRows.reduce((s, r) => s + r['Revenue (SEK)'], 0);
                            const areaYoyRev = areaRows.reduce((s, r) => s + r['YoY Revenue (SEK)'], 0);
                            const areaCost = areaRows.reduce((s, r) => s + r['Cost (SEK)'], 0);
                            const areaClicks = areaRows.reduce((s, r) => s + r.Clicks, 0);
                            const areaCos = areaRev ? areaCost / areaRev * 100 : 0;
                            const areaYoy = areaYoyRev ? (areaRev / areaYoyRev - 1) * 100 : 0;

                            return (
                                <React.Fragment key={area}>
                                    <tr className="portfolio-row-ba">
                                        <td><span style={{ fontWeight: 600 }}>{area}</span><span className="area-badge">Area</span></td>
                                        <td>{fmtNum(areaClicks)}</td>
                                        <td className="td-bold">{fmtNum(areaRev)} SEK</td>
                                        <td>{fmtNum(areaCost)} SEK</td>
                                        <td className="td-bold">{areaCos.toFixed(1)}%</td>
                                        <td>9.0%</td>
                                        <td><PerfBar value={areaYoy} /></td>
                                    </tr>
                                    {Array.from(companies.entries()).map(([co, coRows]) => {
                                        const coRev = coRows.reduce((s, r) => s + r['Revenue (SEK)'], 0);
                                        const coYoyRev = coRows.reduce((s, r) => s + r['YoY Revenue (SEK)'], 0);
                                        const coCost = coRows.reduce((s, r) => s + r['Cost (SEK)'], 0);
                                        const coClicks = coRows.reduce((s, r) => s + r.Clicks, 0);
                                        const coCos = coRev ? coCost / coRev * 100 : 0;
                                        const coYoy = coYoyRev ? (coRev / coYoyRev - 1) * 100 : 0;

                                        return (
                                            <React.Fragment key={co}>
                                                <tr>
                                                    <td><span style={{ paddingLeft: 24, fontWeight: 500 }}>{co}</span></td>
                                                    <td>{fmtNum(coClicks)}</td>
                                                    <td className="td-bold">{fmtNum(coRev)} SEK</td>
                                                    <td>{fmtNum(coCost)} SEK</td>
                                                    <td className="td-bold">{coCos.toFixed(1)}%</td>
                                                    <td>9.0%</td>
                                                    <td><PerfBar value={coYoy} /></td>
                                                </tr>
                                                {coRows.map(r => (
                                                    <tr key={r.Site}>
                                                        <td><span style={{ paddingLeft: 56, color: 'var(--text-muted)' }}>{r.Site}</span></td>
                                                        <td>{r.Clicks.toLocaleString()}</td>
                                                        <td className="td-bold">{r['Revenue (SEK)'].toLocaleString()} SEK</td>
                                                        <td>{r['Cost (SEK)'].toLocaleString()} SEK</td>
                                                        <td className="td-bold">{r['CoS %'].toFixed(1)}%</td>
                                                        <td>9.0%</td>
                                                        <td><PerfBar value={r['Revenue YoY %']} /></td>
                                                    </tr>
                                                ))}
                                            </React.Fragment>
                                        );
                                    })}
                                </React.Fragment>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
