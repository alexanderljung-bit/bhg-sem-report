export default function SettingsPage() {
    return (
        <div>
            <h2 className="section-title">⚙️ Settings</h2>
            <div className="chart-card">
                <div className="chart-card-title">Credentials</div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
                    GCP credentials are configured via environment variables on the backend server.
                    Contact your administrator to update credentials.
                </p>
            </div>
            <div className="chart-card">
                <div className="chart-card-title">About</div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.9rem' }}>
                    BHG SEM Report v2.0 — Next.js + FastAPI<br />
                    Data source: Google Analytics 4 (BigQuery Export) + Google Ads MCC
                </p>
            </div>
        </div>
    );
}
