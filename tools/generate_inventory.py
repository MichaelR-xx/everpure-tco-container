#!/usr/bin/env python3
"""Generate a workload inventory CSV for the Everpure Azure Managed Disk
Visualization Tool from a declarative JSON spec.

A spec describes one or more VM *archetypes* (a workload of a specific type) and,
for each, a *location mapping* — the regions/zones/subscriptions/vnets the VMs
live in. Each archetype is expanded across every subscription x zone x vnet
combination, `num_instances_in_each_subscription_zone_combination` times, and
each instance gets one row per drive in its `drive_config`.

The emitted CSV uses column headers that auto-map to the tool's fields on the
S3 Upload page (the two required fields, Disk Type and Disk Size, always map).

Usage:
    python tools/generate_inventory.py <spec.json> [-o output.csv]

Spec shape (see tools/sample_spec.json):
{
  "customer": "mk_cmm",
  "output": "C:/Users/micha/Downloads/TCO/mk_cmm",   # dir for the CSV (optional)
  "disks": [
    {
      "id": 1,
      "Description": "POST",              # workload/app-tier label
      "vm_name_prefix": "p1",
      "os_type": "Linux",                 # optional, default "Linux"
      "region": "eastus",
      "subscriptions": ["all"],           # location mapping ...
      "zones": ["1"],
      "vnets": ["vnet-01"],               # optional, default ["vnet-01"]
      "scale_by": "instance",
      "num_instances_in_each_subscription_zone_combination": 36,
      "minimum_ec_sku": "V20MP2R2",
      "drive_config": [
        {"drive_type": "Premium_LRS",   "root": true,  "capacityGB": 128},
        {"drive_type": "PremiumV2_LRS", "root": false, "capacityGB": 2048,
         "iops": 16000, "mbps": 600}      # iops/mbps ONLY for provisioned types
      ]
    }
  ]
}
"""
import argparse
import csv
import json
import os
import re
import sys

# Azure managed-disk types that support USER-PROVISIONED IOPS / throughput.
# For every other type, iops/mbps are tier-derived and must not be set here.
PROVISIONED_DISK_TYPES = {"premiumv2_lrs", "ultrassd_lrs"}

# CSV headers — chosen so the tool auto-maps them (the required Disk Type and
# Disk Size always map; VM name maps to "Compute Count (VMs)" on first upload).
COLUMNS = ["vm_name", "description", "region", "zone", "subscription", "vnet",
           "diskType", "capacity", "iops", "mbps", "status", "osType", "root",
           "minimum_ec_sku"]


def _slug(s):
    return re.sub(r"[^A-Za-z0-9]+", "-", str(s)).strip("-").lower() or "x"


def _as_bool_str(v):
    """Accept JSON booleans or "True"/"False" strings; emit "True"/"False"."""
    if isinstance(v, bool):
        return "True" if v else "False"
    return "True" if str(v).strip().lower() in ("true", "1", "yes", "y") else "False"


def _validate_drive(drive, arche_id, drv_index):
    where = f"disks[id={arche_id}].drive_config[{drv_index}]"
    dtype = str(drive.get("drive_type", "")).strip()
    if not dtype:
        raise ValueError(f"{where}: 'drive_type' is required.")
    if "capacityGB" not in drive:
        raise ValueError(f"{where}: 'capacityGB' is required.")
    provisioned = dtype.lower() in PROVISIONED_DISK_TYPES
    has_iops = drive.get("iops") not in (None, "")
    has_mbps = drive.get("mbps") not in (None, "")
    if (has_iops or has_mbps) and not provisioned:
        raise ValueError(
            f"{where}: 'iops'/'mbps' may only be set for provisioned disk types "
            f"({', '.join(sorted(PROVISIONED_DISK_TYPES))}); '{dtype}' is tier-based.")
    return dtype, provisioned


def generate_rows(spec):
    rows = []
    archetypes = spec.get("disks", [])
    if not isinstance(archetypes, list) or not archetypes:
        raise ValueError("Spec has no 'disks' archetypes.")
    for arche in archetypes:
        aid = arche.get("id", "?")
        desc = str(arche.get("Description", arche.get("description", f"grp{aid}")))
        prefix = str(arche.get("vm_name_prefix", "vm"))
        region = str(arche.get("region", "")).strip()
        os_type = str(arche.get("os_type", "Linux"))
        subs = arche.get("subscriptions") or ["all"]
        zones = arche.get("zones") or ["1"]
        vnets = arche.get("vnets") or ["vnet-01"]
        scale_by = str(arche.get("scale_by", "instance")).lower()
        if scale_by != "instance":
            print(f"  warning: disks[id={aid}] scale_by='{scale_by}' not supported; "
                  f"treating as 'instance'.", file=sys.stderr)
        n = int(arche.get("num_instances_in_each_subscription_zone_combination", 1))
        grp_min_sku = str(arche.get("minimum_ec_sku", "none"))
        drives = arche.get("drive_config", [])
        if not region:
            raise ValueError(f"disks[id={aid}]: 'region' is required.")
        if not drives:
            raise ValueError(f"disks[id={aid}]: 'drive_config' is empty.")
        validated = [_validate_drive(d, aid, i) for i, d in enumerate(drives)]

        # Location mapping: expand across every subscription x zone x vnet combo.
        for sub in subs:
            for zone in zones:
                for vnet in vnets:
                    for i in range(n):
                        # Globally-unique, role-distinct VM name (avoids collisions
                        # between archetypes that share a vm_name_prefix).
                        vm_name = f"{prefix}-{_slug(desc)}-{_slug(sub)}-z{_slug(zone)}-{_slug(vnet)}-{i:03d}"
                        for (dtype, provisioned), drive in zip(validated, drives):
                            drv_min_sku = str(drive.get("minimum_ec_sku", "none"))
                            eff_min_sku = drv_min_sku if drv_min_sku.lower() != "none" else grp_min_sku
                            rows.append({
                                "vm_name": vm_name,
                                "description": desc,
                                "region": region,
                                "zone": zone,
                                "subscription": sub,
                                "vnet": vnet,
                                "diskType": dtype,
                                "capacity": drive["capacityGB"],
                                "iops": drive.get("iops", "") if provisioned else "",
                                "mbps": drive.get("mbps", "") if provisioned else "",
                                "status": str(drive.get("status", "Attached")),
                                "osType": os_type,
                                "root": _as_bool_str(drive.get("root", False)),
                                "minimum_ec_sku": eff_min_sku,
                            })
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser(description="Generate a workload inventory CSV from a JSON spec.")
    ap.add_argument("spec", help="path to the JSON spec")
    ap.add_argument("-o", "--output", help="output CSV path (default: <spec.output>/<customer>_inventory.csv)")
    args = ap.parse_args(argv)

    with open(args.spec, encoding="utf-8") as f:
        spec = json.load(f)

    rows = generate_rows(spec)

    # Resolve output path
    out = args.output
    if not out:
        customer = _slug(spec.get("customer", "workload"))
        out_dir = spec.get("output") or "."
        out = os.path.join(out_dir, f"{customer}_inventory.csv")
    out_dir = os.path.dirname(os.path.abspath(out))
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)

    # Summary
    vms = len({r["vm_name"] for r in rows})
    cap = sum(float(r["capacity"] or 0) for r in rows)
    print(f"Wrote {len(rows)} disk rows across {vms} VMs "
          f"(~{cap:,.0f} GB) to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
