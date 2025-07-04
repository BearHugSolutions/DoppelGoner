// components/resolution-mode-switcher.tsx
"use client";

import { useEntityResolution } from "@/context/entity-resolution-context";
import { Button } from "@/components/ui/button";

export type ResolutionMode = "entity" | "service";

export default function ResolutionModeSwitcher() {
  const { resolutionMode, actions } = useEntityResolution();

  const handleModeChange = (mode: ResolutionMode) => {
    actions.setResolutionMode(mode);
  };

  return (
    <div className="flex flex-col sm:flex-row gap-2">
      <Button
        variant={resolutionMode === "entity" ? "default" : "outline"}
        onClick={() => handleModeChange("entity")}
        className="flex items-center justify-center flex-1"
        size="sm"
      >
        <span className="text-xs sm:text-sm truncate text-center flex">
          Organization
        </span>
      </Button>
      <Button
        variant={resolutionMode === "service" ? "default" : "outline"}
        onClick={() => handleModeChange("service")}
        className="flex items-center justify-center flex-1"
        size="sm"
      >
        <span className="text-xs sm:text-sm truncate text-center flex">
          Service
        </span>
      </Button>
    </div>
  );
}
