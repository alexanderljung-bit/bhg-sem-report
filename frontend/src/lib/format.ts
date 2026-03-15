/**
 * Format numbers with K/M suffixes.
 */
export function fmtNum(n: number, prefix = '', suffix = ''): string {
    if (n === 0) return `${prefix}0${suffix}`;
    const abs = Math.abs(n);
    let formatted: string;
    if (abs >= 1_000_000) {
        formatted = (n / 1_000_000).toFixed(1) + 'M';
    } else if (abs >= 1_000) {
        formatted = (n / 1_000).toFixed(1) + 'K';
    } else {
        formatted = n.toLocaleString('sv-SE');
    }
    return `${prefix}${formatted}${suffix}`;
}

/**
 * Calculate YoY percentage.
 */
export function yoyPct(current: number, previous: number): number {
    if (previous === 0) return 0;
    return ((current / previous - 1) * 100);
}
