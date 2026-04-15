import type { Metadata } from "next";
import { Lato } from "next/font/google";
import "./globals.css";
import { TabNav } from "../components/TabNav";

const lato = Lato({
  subsets: ["latin"],
  weight: ["300", "400", "700", "900"],
  variable: "--font-lato",
});

export const metadata: Metadata = {
  title: "Boston 311 Service Request Modeling",
  description:
    "Forecasting municipal service demand across Boston neighborhoods with Bayesian time series models.",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className={lato.variable}>
        <div className="site-shell">
          <header className="site-header">
            <div>
              <p className="eyebrow">DS4420 Final Project</p>
              <h1>Boston 311 Service Request Modeling</h1>
            </div>
            <TabNav />
          </header>
          <main className="site-content">{children}</main>
        </div>
      </body>
    </html>
  );
}
