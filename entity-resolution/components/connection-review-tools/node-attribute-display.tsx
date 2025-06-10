// /components/node-attribute-display.tsx
import React, { JSX, useState } from "react";
import {
  Loader2,
  ChevronDown,
  ChevronUp,
  Phone,
  Briefcase,
  MapPin,
  AlertTriangle,
  Link,
} from "lucide-react";

// REFACTORED: Updated type imports for the unified location/address structure
import type {
  NodeDetailResponse,
  NodePhone,
  NodeServiceAttribute,
  LocationAndAddress, // ADD this
  // REMOVE NodeLocation,
  // REMOVE NodeAddress,
  Organization,
  Service,
} from "@/types/entity-resolution";

import {
  Collapsible,
  CollapsibleTrigger,
  CollapsibleContent,
} from "@/components/ui/collapsible";

// REFACTORED: Merged location and address priority fields and removed the 'address' key
const PRIORITY_FIELDS: Record<string, string[]> = {
  location: [
    "name",
    "transportation",
    "locationType",
    "address1",
    "address2",
    "city",
    "stateProvince",
    "postalCode",
    "country",
    "attention",
  ],
  phone: ["number", "type", "language", "description"],
  service: ["name", "sourceSystem", "updatedAt", "url"],
};

const LOW_PRIORITY_COMMON_FIELDS = [
  "id",
  "originalId",
  "originalTranslationsId",
  "contributorId",
  "organizationId",
  "serviceId",
  "created",
  "createdAt",
  "lastModified",
  "last_modified",
  "country",
  "updatedAt",
  "locationType",
  "latitude",
  "longitude",
];

interface NodeAttributesDisplayProps {
  node: Organization | Service;
  nodeDetails: NodeDetailResponse | null | "loading" | "error";
  isAttributesOpen: boolean;
  setIsAttributesOpen: (open: boolean) => void;
}

const NodeAttributesDisplay: React.FC<NodeAttributesDisplayProps> = ({
  node,
  nodeDetails,
  isAttributesOpen,
  setIsAttributesOpen,
}) => {
  if (nodeDetails === "loading") {
    return (
      <div className="text-xs text-muted-foreground flex items-center">
        <Loader2 className="h-3 w-3 mr-1 animate-spin" /> Loading details...
      </div>
    );
  }

  if (nodeDetails === "error") {
    return (
      <div className="text-xs text-destructive-foreground flex items-center bg-destructive/10 p-2 rounded-md">
        <AlertTriangle className="h-3 w-3 mr-1" /> Error loading details.
      </div>
    );
  }

  if (!nodeDetails) {
    return (
      <div className="text-xs text-muted-foreground">Details unavailable.</div>
    );
  }

  const { baseData, attributes } = nodeDetails;

  const formatValue = (value: any): string => {
    if (value === null || typeof value === "undefined") return "N/A";
    if (typeof value === "boolean") return value ? "Yes" : "No";
    if (value instanceof Date)
      return value.toLocaleDateString() + " " + value.toLocaleTimeString();
    if (
      typeof value === "string" &&
      /\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}/.test(value)
    ) {
      try {
        const date = new Date(value);
        return date.toLocaleDateString() + " " + date.toLocaleTimeString();
      } catch {
        return value;
      }
    }
    return String(value);
  };

  const camelCaseToTitleCase = (text: string) => {
    const result = text.replace(/([A-Z])/g, " $1");
    return result.charAt(0).toUpperCase() + result.slice(1);
  };

  let primaryPhoneNumber: string | null = null;
  if (attributes?.phones && attributes.phones.length > 0) {
    const voicePhone = attributes.phones.find(
      (p) => p.type === "voice" && p.number
    );
    primaryPhoneNumber =
      voicePhone?.number || attributes.phones[0]?.number || null;
  }

  let serviceNames: string[] = [];
  if (attributes?.services && attributes.services.length > 0) {
    serviceNames = attributes.services.map((s) => s.name).filter(Boolean);
  }

  // REFACTORED: Simplified primary address logic using the unified locations array.
  let primaryAddressString: string | null = null;
  if (attributes?.locations && attributes.locations.length > 0) {
    const loc = attributes.locations[0]; // The first location *is* the address
    const addressParts: string[] = [];

    if (loc.address1) {
      let streetPart = loc.address1;
      if (loc.address2) {
        streetPart += `, ${loc.address2}`;
      }
      addressParts.push(streetPart);
    }

    let cityStateZipPart = "";
    if (loc.city) {
      cityStateZipPart += loc.city;
    }
    if (loc.stateProvince) {
      cityStateZipPart += (cityStateZipPart ? ", " : "") + loc.stateProvince;
    }
    if (loc.postalCode) {
      const prefix =
        cityStateZipPart && loc.stateProvince
          ? " "
          : cityStateZipPart
          ? ", "
          : "";
      cityStateZipPart += prefix + loc.postalCode;
    }
    if (cityStateZipPart) {
      addressParts.push(cityStateZipPart);
    }

    if (loc.country) {
      addressParts.push(loc.country);
    }

    primaryAddressString = addressParts.filter(Boolean).join(", ");
  }

  // REFACTORED: Updated the function signature to handle the new unified type.
  const renderAttributeItem = (
    item: NodePhone | NodeServiceAttribute | LocationAndAddress,
    attributeKey: string,
    itemIndex: number
  ) => {
    const itemKeys = Object.keys(item).filter(
      (key) =>
        item[key as keyof typeof item] !== null &&
        item[key as keyof typeof item] !== ""
    );
    const prioritySubKeys = PRIORITY_FIELDS[attributeKey] || [];

    const primaryContent: JSX.Element[] = [];
    const secondaryContent: JSX.Element[] = [];

    itemKeys.forEach((subKey) => {
      const formattedSubKey = camelCaseToTitleCase(subKey);
      const displayValue = formatValue(item[subKey as keyof typeof item]);

      if (displayValue === "N/A" && !prioritySubKeys.includes(subKey)) {
        return;
      }

      const isUrl =
        subKey === "url" &&
        typeof displayValue === "string" &&
        (displayValue.startsWith("http://") ||
          displayValue.startsWith("https://"));

      const element = (
        <div key={subKey} className="mb-1">
          <span className="font-medium capitalize text-gray-700">
            {formattedSubKey}:
          </span>
          {isUrl ? (
            <a
              href={displayValue}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-600 hover:underline ml-1 break-all"
            >
              {displayValue}
            </a>
          ) : (
            <span className="text-gray-600 ml-1 break-all">{displayValue}</span>
          )}
        </div>
      );

      if (
        prioritySubKeys.includes(subKey) ||
        !LOW_PRIORITY_COMMON_FIELDS.includes(subKey)
      ) {
        primaryContent.push(element);
      } else {
        secondaryContent.push(element);
      }
    });

    return (
      <li
        key={"id" in item ? item.id : `${attributeKey}-${itemIndex}`}
        className="py-2 px-3 mb-2 bg-slate-50 rounded-md shadow-sm border border-slate-200"
      >
        <div className="space-y-1">{primaryContent}</div>
        {secondaryContent.length > 0 && (
          <CollapsibleItem title="More details" initiallyOpen={false}>
            <div className="space-y-1 mt-1 pt-1 border-t border-slate-200">
              {secondaryContent}
            </div>
          </CollapsibleItem>
        )}
      </li>
    );
  };

  const CollapsibleItem: React.FC<{
    title: string;
    children: React.ReactNode;
    initiallyOpen?: boolean;
  }> = ({ title, children, initiallyOpen = false }) => {
    const [isOpen, setIsOpen] = useState(initiallyOpen);
    return (
      <div>
        <button
          onClick={() => setIsOpen(!isOpen)}
          className="text-xs text-blue-600 hover:text-blue-800 flex items-center mt-2 py-1"
        >
          {isOpen ? (
            <ChevronUp size={14} className="mr-1" />
          ) : (
            <ChevronDown size={14} className="mr-1" />
          )}
          {title}
        </button>
        {isOpen && <div className="mt-1 text-xs">{children}</div>}
      </div>
    );
  };

  const displayedAttributesCount = attributes
    ? Object.entries(attributes).filter(([attrKey, attrValues]) => {
        // Core info is now pulled from multiple sources, so we adjust filtering
        if (attrKey === "phones" && primaryPhoneNumber) return false;
        if (attrKey === "services" && serviceNames.length > 0) return false;
        // The primary address is now part of the first location
        if (
          attrKey === "locations" &&
          primaryAddressString &&
          attrValues?.length === 1
        )
          return false;
        // The addresses key no longer exists
        return Array.isArray(attrValues) && attrValues.length > 0;
      }).length
    : 0;

  return (
    <div className="space-y-2 text-sm p-3 bg-white rounded-lg shadow">
      <div className="pb-2 mb-3 border-b border-gray-200">
        <h4 className="font-semibold text-base text-slate-800 mb-2">
          Core Information
        </h4>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-x-4 gap-y-1.5 text-xs">
          <div>
            <span className="font-medium text-slate-600">ID:</span>
            <span className="text-slate-500 ml-1">{baseData.id}</span>
          </div>
          <div className="sm:col-span-2">
            <span className="font-medium text-slate-600">Name:</span>
            <span className="text-slate-800 font-semibold ml-1">
              {baseData.name || "N/A"}
            </span>
          </div>

          {"url" in node && node.url && (
            <div className="sm:col-span-2">
              <span className="font-medium text-slate-600 flex items-center">
                <Link size={12} className="mr-1.5 text-slate-500" />
                URL:
              </span>
              <a
                href={node.url as string}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-600 hover:underline ml-1 break-all"
              >
                {node.url}
              </a>
            </div>
          )}

          <div>
            <span className="font-medium text-slate-600">Source System:</span>
            <span className="text-slate-500 ml-1">
              {baseData.sourceSystem || "N/A"} ({baseData.sourceId || "N/A"})
            </span>
          </div>
          {primaryPhoneNumber && (
            <div>
              <span className="font-medium text-slate-600 flex items-center">
                <Phone size={12} className="mr-1.5 text-slate-500" />
                Phone:
              </span>
              <span className="text-slate-700 ml-1">{primaryPhoneNumber}</span>
            </div>
          )}
          {primaryAddressString && (
            <div className="sm:col-span-2">
              <span className="font-medium text-slate-600 flex items-center">
                <MapPin size={12} className="mr-1.5 text-slate-500" />
                Address:
              </span>
              <span className="text-slate-700 ml-1">
                {primaryAddressString}
              </span>
            </div>
          )}
          {serviceNames.length > 0 && (
            <div className="sm:col-span-2">
              <span className="font-medium text-slate-600 flex items-center">
                <Briefcase size={12} className="mr-1.5 text-slate-500" />
                Services:
              </span>
              <span className="text-slate-700 ml-1">
                {serviceNames.length <= 2
                  ? serviceNames.join(", ")
                  : `${serviceNames.slice(0, 2).join(", ")}, and ${
                      serviceNames.length - 2
                    } more`}
              </span>
            </div>
          )}
        </div>
      </div>

      {attributes && displayedAttributesCount > 0 && (
        <Collapsible
          open={isAttributesOpen}
          onOpenChange={setIsAttributesOpen}
          className="mt-3"
        >
          <CollapsibleTrigger className="flex items-center justify-between w-full p-2 text-sm font-medium text-left text-blue-700 bg-blue-50 hover:bg-blue-100 rounded-md focus:outline-none focus-visible:ring focus-visible:ring-blue-500 focus-visible:ring-opacity-75">
            <span className="font-semibold">
              Additional Attributes ({displayedAttributesCount})
            </span>
            {isAttributesOpen ? (
              <ChevronUp className="h-5 w-5 text-blue-700" />
            ) : (
              <ChevronDown className="h-5 w-5 text-blue-700" />
            )}
          </CollapsibleTrigger>
          <CollapsibleContent>
            <div className="space-y-3 mt-2 pt-3 border-t border-gray-200">
              {Object.entries(attributes).map(([attrKey, attrValues]) => {
                if (attrKey === "phones" && primaryPhoneNumber) return null;
                if (attrKey === "services" && serviceNames.length > 0)
                  return null;
                if (
                  attrKey === "locations" &&
                  primaryAddressString &&
                  attrValues?.length === 1
                )
                  return null;

                if (!Array.isArray(attrValues) || attrValues.length === 0) {
                  return null;
                }
                const humanReadableAttrKey = camelCaseToTitleCase(attrKey);
                return (
                  <div key={attrKey} className="pt-2">
                    <h5 className="font-semibold capitalize text-slate-800 mb-1 text-base">
                      {humanReadableAttrKey}
                      <span className="text-xs font-normal text-slate-500 ml-1">
                        ({attrValues.length}{" "}
                        {attrValues.length === 1 ? "item" : "items"})
                      </span>
                    </h5>
                    <ul className="space-y-0 list-none p-0">
                      {attrValues.map((val, valIndex) =>
                        renderAttributeItem(val, attrKey, valIndex)
                      )}
                    </ul>
                  </div>
                );
              })}
            </div>
          </CollapsibleContent>
        </Collapsible>
      )}
      {(!attributes || displayedAttributesCount === 0) && (
        <p className="text-xs text-muted-foreground mt-4">
          No additional attributes to display.
        </p>
      )}
    </div>
  );
};

export default NodeAttributesDisplay;
