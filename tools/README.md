# Workload inventory generator

`generate_inventory.py` turns a declarative JSON spec into the workload inventory CSV that the
tool ingests on the **S3 Upload** page. Use it to build a synthetic workload of a specific type
and place it at a location mapping (region / zone / subscription / vnet).

## Usage

```bash
python tools/generate_inventory.py tools/sample_spec.json -o mk_cmm_inventory.csv
```

If `-o` is omitted, the CSV is written to `<spec.output>/<customer>_inventory.csv`.

Then in the app: **S3 Upload → drop the CSV → Parse**. The headers auto-map to the tool's fields
(the two required fields, *Disk Type* and *Disk Size*, always map; VM name maps to *Compute Count
(VMs)*). `description` and `minimum_ec_sku` are extra info columns and stay unmapped.

## Spec format

Top level: `customer`, optional `output` (directory for the CSV), and `disks` — a list of VM
**archetypes**. Each archetype:

| Field | Meaning |
|---|---|
| `id`, `Description` | identifier and workload/app-tier label (e.g. POST, CMM, MSSQL) |
| `vm_name_prefix` | base of the generated VM names (made unique per role/location) |
| `os_type` | optional, default `Linux` |
| `region` | Azure region |
| `subscriptions`, `zones`, `vnets` | **location mapping** — expanded as a cartesian product |
| `scale_by` | `instance` (only supported mode) |
| `num_instances_in_each_subscription_zone_combination` | VMs per location combination |
| `minimum_ec_sku` | group-level minimum EC SKU (drive-level overrides when not `"none"`) |
| `drive_config` | list of drives each VM gets |

Each drive:

| Field | Meaning |
|---|---|
| `drive_type` | Azure disk type, e.g. `Premium_LRS`, `StandardSSD_LRS`, `PremiumV2_LRS`, `UltraSSD_LRS` |
| `root` | boolean — is this the OS/root disk |
| `capacityGB` | provisioned size in GB |
| `iops`, `mbps` | **provisioned IOPS / throughput — only valid for `PremiumV2_LRS` and `UltraSSD_LRS`** (Premium SSD v2 and Ultra Disk). Setting them on any other, tier-based type is a validation error. |
| `minimum_ec_sku` | optional per-drive override |

### Expansion

For each archetype, the generator produces
`len(subscriptions) × len(zones) × len(vnets) × num_instances` VMs, and one CSV row per drive on
each VM. VM names embed the role and location so archetypes that share a `vm_name_prefix` never
collide.

See `sample_spec.json` for a complete example (39 VMs → 156 disk rows).
