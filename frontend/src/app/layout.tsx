'use client';
import { useState, createContext, useContext } from 'react';
import Navbar from '@/components/Navbar';
import './globals.css';

// We import Outfit and JetBrains Mono in globals.css, so no next/font needed here if using standard @import.

interface DateContextType {
    startDate: string;
    endDate: string;
}

export const DateContext = createContext<DateContextType>({ startDate: '', endDate: '' });
export const useDates = () => useContext(DateContext);

export default function RootLayout({ children }: { children: React.ReactNode }) {
    const [dates, setDates] = useState<DateContextType>({ startDate: '', endDate: '' });

    return (
        <html lang="sv">
            <head>
                <title>BHG SEM Report</title>
                <meta name="description" content="BHG Group SEM Performance Dashboard" />
            </head>
            <body>
                <DateContext.Provider value={dates}>
                    <Navbar onDatesChange={(start, end) => setDates({ startDate: start, endDate: end })} />
                    <main className="content">
                        {children}
                    </main>
                </DateContext.Provider>
            </body>
        </html>
    );
}
