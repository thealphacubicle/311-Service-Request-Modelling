"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const tabs = [
  { href: "/", label: "Project Overview" },
  { href: "/results", label: "Interactive Results" },
];

export function TabNav() {
  const pathname = usePathname();

  return (
    <nav className="tab-nav" aria-label="Primary tabs">
      {tabs.map((tab) => {
        const active = pathname === tab.href;
        return (
          <Link
            key={tab.href}
            href={tab.href}
            className={`tab-link ${active ? "active" : ""}`}
          >
            {tab.label}
          </Link>
        );
      })}
    </nav>
  );
}
