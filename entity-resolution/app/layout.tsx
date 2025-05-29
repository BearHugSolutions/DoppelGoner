// app/layout.tsx
import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { ThemeProvider } from "@/components/theme-provider"; // Assuming you have this
import { AuthProvider } from "@/context/auth-context"; // Import AuthProvider and useAuth
import AppContent from "@/components/auth/app-content";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Entity Resolution Platform",
  description: "Review and manage entity clusters.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning className="h-full">
      <body className={`${inter.className} h-full`}>
        <AuthProvider>
          <ThemeProvider
            attribute="class"
            defaultTheme="system"
            enableSystem
            disableTransitionOnChange
          >
            <AppContent>{children}</AppContent>
          </ThemeProvider>
        </AuthProvider>
      </body>
    </html>
  );
}