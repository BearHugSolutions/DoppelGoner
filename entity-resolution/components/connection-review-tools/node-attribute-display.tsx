// /components/connection-review-tools/node-attribute-display.tsx
import React, { JSX, useState } from "react";
import {
  Loader2,
  ChevronDown,
  ChevronUp,
  Phone,
  Briefcase,
  MapPin, // Added MapPin icon
  AlertTriangle, // Added for error state
} from "lucide-react";
import type { NodeDetailResponse } from "@/types/entity-resolution"; // Assuming this path is correct

// Assuming your Radix-based Collapsible components are here:
import {
  Collapsible,
  CollapsibleTrigger,
  CollapsibleContent,
} from "@/components/ui/collapsible"; // Adjust path if necessary

// Define a list of high-priority keys for each attribute type
const PRIORITY_FIELDS: Record<string, string[]> = {
  location: [
    "name",
    "latitude",
    "longitude",
    "transportation",
    "location_type",
  ],
  phone: ["number", "type", "language", "description"],
  service: [
    "name",
    "description",
    "short_description",
    "application_process",
    "eligibility_description",
    "url",
    "email",
    "alternate_name",
    "status",
    "alert",
  ],
  organization: ["name", "id"],
  address: [
    "address_1",
    "address_2",
    "city",
    "state_province",
    "postal_code",
    "country",
    "address_type",
    "attention",
  ],
};

const LOW_PRIORITY_COMMON_FIELDS = [
  "id",
  "original_id",
  "original_translations_id",
  "contributor_id",
  "organization_id",
  "service_id",
  "created",
  "created_at",
  "last_modified",
  "updated_at",
  "embedding_v2_updated_at",
  "maximum_age",
  "fees_description",
];

interface NodeAttributesDisplayProps {
  nodeDetails: NodeDetailResponse | null | "loading" | "error"; // Updated to include "error"
}

const NodeAttributesDisplay: React.FC<NodeAttributesDisplayProps> = ({
  nodeDetails,
}) => {
  const [isAttributesOpen, setIsAttributesOpen] = useState(false);

  if (nodeDetails === "loading") {
    return (
      <div className="text-xs text-muted-foreground flex items-center">
        <Loader2 className="h-3 w-3 mr-1 animate-spin" /> Loading details...
      </div>
    );
  }
  // Handle error state
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
      /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)
    ) {
      try {
        const date = new Date(value);
        return date.toLocaleDateString() + " " + date.toLocaleTimeString();
      } catch {}
    }
    return String(value);
  };

  let primaryPhoneNumber: string | null = null;
  if (
    attributes?.phone &&
    Array.isArray(attributes.phone) &&
    attributes.phone.length > 0
  ) {
    const voicePhone = attributes.phone.find(
      (p) => p.type === "voice" && p.number
    );
    if (voicePhone) {
      primaryPhoneNumber = voicePhone.number;
    } else if (attributes.phone[0]?.number) {
      primaryPhoneNumber = attributes.phone[0].number;
    }
  }

  let serviceNames: string[] = [];
  if (attributes?.service && Array.isArray(attributes.service)) {
    serviceNames = attributes.service.map((s) => s.name).filter((name) => name);
  }

  // Extract primary address for Core Information
  let primaryAddressString: string | null = null;
  if (
    attributes?.address &&
    Array.isArray(attributes.address) &&
    attributes.address.length > 0
  ) {
    const addr = attributes.address[0]; // Take the first address
    const addressParts: string[] = [];

    // Street part (address_1 and optional address_2)
    if (addr.address_1) {
      let streetPart = addr.address_1;
      if (addr.address_2) {
        streetPart += `, ${addr.address_2}`;
      }
      addressParts.push(streetPart);
    }

    // City, State ZIP part
    let cityStateZipPart = "";
    if (addr.city) {
      cityStateZipPart += addr.city;
    }
    if (addr.state_province) {
      cityStateZipPart += (cityStateZipPart ? ", " : "") + addr.state_province;
    }
    if (addr.postal_code) {
      // Add space if state_province was just added, otherwise add a comma if cityStateZipPart is not empty
      const prefix = (cityStateZipPart && cityStateZipPart.endsWith(addr.state_province || '')) ? " " : (cityStateZipPart ? ", " : "");
      cityStateZipPart += prefix + addr.postal_code;
    }
    if (cityStateZipPart) {
      addressParts.push(cityStateZipPart);
    }
    
    // Country part
    if (addr.country) {
      addressParts.push(addr.country);
    }

    primaryAddressString = addressParts.filter(Boolean).join(', ');
  }


  const renderAttributeItem = (
    item: any,
    attributeKey: string,
    itemIndex: number
  ) => {
    const itemKeys = Object.keys(item).filter(
      (key) => item[key] !== null && item[key] !== ""
    );
    const prioritySubKeys = PRIORITY_FIELDS[attributeKey] || [];

    const primaryContent: JSX.Element[] = [];
    const secondaryContent: JSX.Element[] = [];

    itemKeys.forEach((subKey) => {
      const formattedSubKey = subKey
        .replace(/_/g, " ")
        .replace(/([A-Z])/g, " $1")
        .trim();
      const displayValue = formatValue(item[subKey]);

      if (
        displayValue === "N/A" &&
        !prioritySubKeys.includes(subKey) &&
        subKey !== "alternate_name" &&
        subKey !== "description" &&
        subKey !== "short_description"
      ) {
        return;
      }

      const element = (
        <div key={subKey} className="mb-1">
          <span className="font-medium capitalize text-gray-700">
            {formattedSubKey}:
          </span>
          <span className="text-gray-600 ml-1 break-all">{displayValue}</span>
        </div>
      );

      if (
        prioritySubKeys.includes(subKey) ||
        (!LOW_PRIORITY_COMMON_FIELDS.includes(subKey) &&
          !subKey.endsWith("_id") &&
          !subKey.includes("date") &&
          !subKey.includes("created") &&
          !subKey.includes("modified") &&
          subKey !== "geom")
      ) {
        primaryContent.push(element);
      } else {
        if (subKey !== "geom") {
          secondaryContent.push(element);
        }
      }
    });

    if (
      attributeKey === "service" &&
      item.alternate_name &&
      typeof item.alternate_name === "string"
    ) {
      const altNamesRaw = item.alternate_name.split(",");
      if (altNamesRaw.length > 1 || item.alternate_name.length > 100) {
        const altNames = altNamesRaw
          .map((an: string) => an.trim())
          .filter((an: string) => an);
        if (altNames.length > 0) {
          const altNameIndex = primaryContent.findIndex(
            (el) => el.key === "alternate_name"
          );
          const contentToReplaceWithList = (
            <div key="alternate_name_list" className="mt-1 w-full">
              <span className="font-medium capitalize text-gray-700">
                Alternate Names:
              </span>
              <ul className="list-disc list-inside ml-1 space-y-0.5">
                {altNames.map((an: string, i: number) => (
                  <li
                    key={`${attributeKey}-altname-${itemIndex}-${i}`}
                    className="text-gray-600 text-xs"
                  >
                    {an}
                  </li>
                ))}
              </ul>
            </div>
          );
          if (altNameIndex !== -1) {
            primaryContent[altNameIndex] = contentToReplaceWithList;
          } else {
            primaryContent.push(contentToReplaceWithList);
          }
        }
      }
    }

    return (
      <li
        key={item.id || `${attributeKey}-${itemIndex}`}
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
        if (attrKey === "phone" && primaryPhoneNumber) return false;
        if (attrKey === "service" && serviceNames.length > 0) return false;
        // If primaryAddressString is set, it means attributes.address[0] is shown in Core Info.
        // So, we skip the 'address' key from "Additional Attributes" count and rendering.
        if (attrKey === "address" && primaryAddressString) return false;
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
          <div>
            <span className="font-medium text-slate-600">Source System:</span>
            <span className="text-slate-500 ml-1">
              {baseData.source_system || "N/A"} ({baseData.source_id || "N/A"})
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
          {/* Display Primary Address */}
          {primaryAddressString && (
            <div className="sm:col-span-2">
              <span className="font-medium text-slate-600 flex items-center">
                <MapPin size={12} className="mr-1.5 text-slate-500" />
                Address:
              </span>
              <span className="text-slate-700 ml-1">{primaryAddressString}</span>
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
              {serviceNames.length > 2 && (
                <CollapsibleItem
                  title="View all services"
                  initiallyOpen={false}
                >
                  <ul className="list-disc list-inside ml-2 mt-1">
                    {serviceNames.map((name, index) => (
                      <li
                        key={`service-name-${index}`}
                        className="text-slate-600"
                      >
                        {name}
                      </li>
                    ))}
                  </ul>
                </CollapsibleItem>
              )}
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
                if (attrKey === "phone" && primaryPhoneNumber) return null;
                if (attrKey === "service" && serviceNames.length > 0) return null;
                // If primaryAddressString is set, skip rendering 'address' in additional attributes
                if (attrKey === "address" && primaryAddressString) return null;

                if (!Array.isArray(attrValues) || attrValues.length === 0) {
                  return null;
                }
                const humanReadableAttrKey = attrKey
                  .replace(/_/g, " ")
                  .replace(/([A-Z])/g, " $1")
                  .trim();
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
                      {attrValues.map((val: any, valIndex: number) =>
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
