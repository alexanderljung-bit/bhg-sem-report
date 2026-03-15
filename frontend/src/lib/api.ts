const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

async function fetchApi<T>(path: string, options?: RequestInit): Promise<T> {
    const res = await fetch(`${API_BASE}${path}`, {
        headers: { 'Content-Type': 'application/json' },
        ...options,
    });
    if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }));
        throw new Error(err.detail || res.statusText);
    }
    return res.json();
}

// --- Types ---
export interface KpiData {
    clicks: number; revenue: number; cost: number; cos: number; transactions: number;
    yoy_clicks: number; yoy_revenue: number; yoy_cost: number; yoy_cos: number; yoy_transactions: number;
}

export interface SegmentRow {
    Segment: string; Clicks: number; Transactions: number; 'CR %': number;
    'Revenue (SEK)': number; 'Cost (SEK)': number; 'CoS %': number; 'Revenue YoY %': number;
}

export interface WeeklyRow {
    Week: string; Clicks: number; 'Revenue (SEK)': number;
    'Cost (SEK)': number; 'CoS %': number; 'Revenue YoY %': number;
}

export interface DailyRow {
    Date: string; Revenue: number; Cost: number; 'CoS %': number;
}

export interface CumulativeRow {
    Date: string; 'Cumulative Revenue': number; 'Cumulative Cost': number;
    'Cumulative CoS %': number; 'Budget Target %': number;
}

export interface PortfolioRow {
    'Business Area': string; Company: string; Site: string;
    Clicks: number; 'Revenue (SEK)': number; 'YoY Revenue (SEK)': number;
    'Cost (SEK)': number; 'CoS %': number; 'Revenue YoY %': number;
}

export interface Source {
    dataset_id: string; label: string; ga4_property_id: string;
    business_area: string; company: string; gads_customer_id: string;
    vat_status: string; currency: string; status: string;
}

// --- API Functions ---
export async function getDeepDive(start: string, end: string, company: string, site: string) {
    const params = new URLSearchParams({ start, end, company, site });
    return fetchApi<{ kpi: KpiData; segments: SegmentRow[]; weekly: WeeklyRow[]; daily: DailyRow[]; cumulative: CumulativeRow[] }>(
        `/api/deep-dive?${params}`
    );
}

export async function getPortfolio(start: string, end: string) {
    const params = new URLSearchParams({ start, end });
    return fetchApi<{ rows: PortfolioRow[] }>(`/api/portfolio?${params}`);
}

export async function getHierarchy() {
    return fetchApi<{ hierarchy: Record<string, Record<string, string[]>>; companies: string[] }>(
        '/api/hierarchy'
    );
}

export async function getSources() {
    return fetchApi<{ sources: Source[]; business_areas: string[]; companies: string[] }>(
        '/api/sources'
    );
}

export async function addSource(data: {
    dataset_id: string; label: string; business_area: string;
    company: string; vat_status: string; gads_customer_id: string;
}) {
    return fetchApi('/api/sources', { method: 'POST', body: JSON.stringify(data) });
}

export async function deleteSource(datasetId: string) {
    return fetchApi(`/api/sources/${datasetId}`, { method: 'DELETE' });
}

export async function testConnection(datasetId: string) {
    return fetchApi<{ success: boolean; table_count: number; first_date: string; last_date: string }>(
        '/api/sources/test', { method: 'POST', body: JSON.stringify({ dataset_id: datasetId }) }
    );
}

export async function discoverDatasets() {
    return fetchApi<{ datasets: { dataset_id: string; ga4_property_id: string }[] }>(
        '/api/sources/discover'
    );
}

export async function getDatePresets() {
    return fetchApi<{ presets: string[] }>('/api/dates/presets/list');
}

export async function getPresetDates(preset: string) {
    return fetchApi<{ start: string; end: string }>(`/api/dates/presets?preset=${encodeURIComponent(preset)}`);
}
