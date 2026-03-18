import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "CSTGlobal — Construction Lead Intelligence",
  description: "Global infrastructure project discovery and lead tracking platform",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body style={{ margin: 0, padding: 0, background: "#0A0F1E" }}>
        {children}
      </body>
    </html>
  );
}
