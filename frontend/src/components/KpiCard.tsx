import { fmtNum, yoyPct } from '@/lib/format';
import YoyBadge from './YoyBadge';

interface KpiCardProps {
    title: string;
    value: number;
    yoyValue: number;
    icon?: string;
    prefix?: string;
    suffix?: string;
    invertYoy?: boolean;
}

export default function KpiCard({ title, value, yoyValue, prefix = '', suffix = '', invertYoy = false }: KpiCardProps) {
    const pct = yoyPct(value, yoyValue);
    return (
        <div className="kpi-card">
            <div className="kpi-title">{title}</div>
            <div className="kpi-value">{fmtNum(value, prefix, suffix)}</div>
            <div className="kpi-yoy-row">
                <YoyBadge value={invertYoy ? -pct : pct} />
            </div>
        </div>
    );
}
