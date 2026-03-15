'use client';
import { useState, createContext, useContext } from 'react';
import Navbar from '@/components/Navbar';
import './globals.css';

interface DateContextType {
    startDate: string;
    endDate: string;
}

interface DeepDiveContextType {
    company: string;
    site: string;
    setCompany: (c: string) => void;
    setSite: (s: string) => void;
}

export const DateContext = createContext<DateContextType>({ startDate: '', endDate: '' });
export const useDates = () => useContext(DateContext);

export const DeepDiveContext = createContext<DeepDiveContextType>({
    company: '', site: '', setCompany: () => {}, setSite: () => {},
});
export const useDeepDive = () => useContext(DeepDiveContext);

export default function RootLayout({ children }: { children: React.ReactNode }) {
    const [dates, setDates] = useState<DateContextType>({ startDate: '', endDate: '' });
    const [company, setCompany] = useState('');
    const [site, setSite] = useState('');

    return (
        <html lang="sv">
            <head>
                <title>BHG SEM Report</title>
                <meta name="description" content="BHG Group SEM Performance Dashboard" />
            </head>
            <body>
                <DateContext.Provider value={dates}>
                    <DeepDiveContext.Provider value={{ company, site, setCompany, setSite }}>
                        <Navbar onDatesChange={(start, end) => setDates({ startDate: start, endDate: end })} />
                        <main className="content">
                            {children}
                        </main>
                    </DeepDiveContext.Provider>
                </DateContext.Provider>
            </body>
        </html>
    );
}
