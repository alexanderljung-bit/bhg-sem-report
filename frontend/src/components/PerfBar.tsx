interface PerfBarProps {
    value: number;
    maxAbs?: number;
}

export default function PerfBar({ value, maxAbs = 30 }: PerfBarProps) {
    const clamped = Math.max(Math.min(value, maxAbs), -maxAbs);
    const width = `${Math.abs(clamped) / maxAbs * 50}%`;
    const isPos = value >= 0;

    return (
        <div className="perf-bar">
            <span className={`perf-label ${isPos ? 'perf-label-pos' : 'perf-label-neg'}`}>
                {isPos ? '+' : ''}{value.toFixed(1)}%
            </span>
            <div className="perf-track">
                <div className="perf-track-center" />
                {isPos ? (
                    <div className="perf-fill-pos" style={{ width }} />
                ) : (
                    <div className="perf-fill-neg" style={{ width }} />
                )}
            </div>
        </div>
    );
}
