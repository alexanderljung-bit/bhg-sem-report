interface YoyBadgeProps {
    value: number;
}

export default function YoyBadge({ value }: YoyBadgeProps) {
    if (value === 0) return <span className="yoy-badge yoy-neutral">0%</span>;
    const cls = value > 0 ? 'yoy-positive' : 'yoy-negative';
    const arrow = value > 0 ? '▲' : '▼';
    return (
        <span className={`yoy-badge ${cls}`}>
            {arrow} {Math.abs(value).toFixed(1)}%
        </span>
    );
}
