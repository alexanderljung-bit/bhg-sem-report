'use client';
import { useState, useEffect } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { getPresetDates } from '@/lib/api';

const PRESETS = ['MTD', 'Last 7 Days', 'Last 30 Days', 'Last Month', 'QTD', 'YTD'];

interface NavbarProps {
    onDatesChange: (start: string, end: string) => void;
}

export default function Navbar({ onDatesChange }: NavbarProps) {
    const pathname = usePathname();
    const [preset, setPreset] = useState('MTD');
    const [menuOpen, setMenuOpen] = useState(false);

    useEffect(() => {
        getPresetDates(preset).then(d => onDatesChange(d.start, d.end));
    }, [preset]);

    return (
        <nav className="navbar">
            <div className="navbar-brand">
                <img src="/bhg-logo.png" alt="BHG" style={{ height: 28 }} />
                <span>SEM Report</span>
            </div>

            <div className="navbar-center">
                <select value={preset} onChange={e => setPreset(e.target.value)}>
                    {PRESETS.map(p => (
                        <option key={p} value={p}>{p}</option>
                    ))}
                </select>
            </div>

            <div className="navbar-menu">
                <Link href="/" className={`btn btn-sm ${pathname === '/' ? 'btn-primary' : ''}`}>
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 4 }}><rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect><line x1="3" y1="9" x2="21" y2="9"></line><line x1="9" y1="21" x2="9" y2="9"></line></svg>
                    Deep-Dive
                </Link>
                <Link href="/group" className={`btn btn-sm ${pathname === '/group' ? 'btn-primary' : ''}`}>
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ marginRight: 4 }}><rect x="4" y="4" width="16" height="16" rx="2" ry="2"></rect><rect x="9" y="9" width="6" height="6"></rect><line x1="9" y1="1" x2="9" y2="4"></line><line x1="15" y1="1" x2="15" y2="4"></line><line x1="9" y1="20" x2="9" y2="23"></line><line x1="15" y1="20" x2="15" y2="23"></line><line x1="20" y1="9" x2="23" y2="9"></line><line x1="20" y1="14" x2="23" y2="14"></line><line x1="1" y1="9" x2="4" y2="9"></line><line x1="1" y1="14" x2="4" y2="14"></line></svg>
                    Group
                </Link>
                <button className="btn btn-sm" onClick={() => setMenuOpen(!menuOpen)}>☰</button>
                {menuOpen && (
                    <div style={{
                        position: 'absolute', top: '100%', right: '2rem', background: 'var(--surface)',
                        border: '1px solid var(--border)', borderRadius: 'var(--radius-md)',
                        boxShadow: 'var(--shadow-lg)', padding: '8px 0', zIndex: 200,
                    }}>
                        <Link href="/sources" className="btn btn-sm" style={{ width: '100%', border: 'none', borderRadius: 0, justifyContent: 'flex-start' }}
                            onClick={() => setMenuOpen(false)}>
                            🔗 Data Sources
                        </Link>
                        <Link href="/settings" className="btn btn-sm" style={{ width: '100%', border: 'none', borderRadius: 0, justifyContent: 'flex-start' }}
                            onClick={() => setMenuOpen(false)}>
                            ⚙️ Settings
                        </Link>
                    </div>
                )}
            </div>
        </nav>
    );
}
