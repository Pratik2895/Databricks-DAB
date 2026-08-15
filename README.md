# Databricks Asset Bundles (DAB) Template

[![Databricks](https://img.shields.io/badge/Databricks-Asset_Bundle-FF3621?style=flat-square&logo=databricks)](https://www.databricks.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg?style=flat-square)](LICENSE)

A production-ready Databricks Asset Bundle (DAB) template implementing **Medallion Architecture** (Bronze → Silver → Gold) with **multi-environment deployment** support.

## 🏗️ Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   SOURCE    │  →   │   BRONZE    │  →   │   SILVER    │  →   │    GOLD     │
│             │      │  (Raw Data) │      │  (Cleansed) │      │ (Business)  │
└─────────────┘      └─────────────┘      └─────────────┘      └─────────────┘
                            ↓                     ↓                     ↓
                     Minimal Transform      Data Quality         Aggregations
                     Audit Metadata        Deduplication           KPIs/Metrics
                     Raw Preservation      Standardization      BI-Ready Tables
```

## ✨ Features

- ✅ **Multi-Environment Deployment** (Dev, Staging, Production)
- ✅ **Medallion Architecture** with notebook-based jobs
- ✅ **Data Quality Validation** with automated checks
- ✅ **Delta Lake** optimization and management
- ✅ **Parameterized Workflows** via Unity Catalog variables
- ✅ **Automated Scheduling** with environment-specific cron
- ✅ **Email Notifications** for job failures/success
- ✅ **Best Practice Tags** for cost tracking and governance

## 📁 Project Structure

```
.
├── databricks.yml                 # Main bundle configuration
├── resources/
│   └── jobs.yml                   # Job definitions (Bronze → Silver → Gold)
├── src/
│   ├── bronze_ingestion.py        # Bronze: Raw data ingestion
│   ├── silver_transformation.py   # Silver: Data cleansing & transformation
│   ├── gold_aggregation.py        # Gold: Business aggregations
│   └── data_quality_check.py      # Data quality validation
├── .gitignore
└── README.md
```

## 🚀 Quick Start

### Prerequisites

1. **Databricks CLI** installed:
   ```bash
   pip install databricks-cli
   ```

2. **Databricks workspace** with:
   - Unity Catalog enabled
   - Appropriate catalog/schema permissions
   - Serverless compute or cluster access

3. **Authentication**:
   ```bash
   databricks configure --token
   ```
   Or use environment variables:
   ```bash
   export DATABRICKS_HOST="https://dbc-316f5fb6-3c9c.cloud.databricks.com"
   export DATABRICKS_TOKEN="your-token"
   ```

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Pratik2895/Databricks-DAB.git
   cd Databricks-DAB
   ```

2. **Update configuration** in `databricks.yml`:
   - Set your workspace URL
   - Configure catalog/schema names
   - Adjust compute settings

## 🎯 Deployment

### Deploy to Development

```bash
# Validate bundle configuration
databricks bundle validate -t dev

# Deploy to dev environment
databricks bundle deploy -t dev

# Run the medallion pipeline
databricks bundle run -t dev medallion_pipeline_job
```

### Deploy to Staging

```bash
databricks bundle deploy -t staging
databricks bundle run -t staging medallion_pipeline_job
```

### Deploy to Production

```bash
databricks bundle deploy -t prod
databricks bundle run -t prod medallion_pipeline_job
```

## 🔧 Configuration

### Environment Variables

Customize behavior per environment in `databricks.yml`:

| Variable | Description | Default | Dev | Staging | Prod |
|----------|-------------|---------|-----|---------|------|
| `catalog` | Unity Catalog name | `main` | `dev_catalog` | `staging_catalog` | `prod_catalog` |
| `schema` | Schema name | `default` | `${user}` | `analytics` | `analytics` |
| `node_type` | Cluster node type | `i3.xlarge` | `i3.xlarge` | `i3.xlarge` | `r5.2xlarge` |
| `spark_version` | DBR version | `13.3.x-scala2.12` | Same | Same | Same |

### Schedule Configuration

| Environment | Schedule | Status | Notifications |
|-------------|----------|--------|---------------|
| **Dev** | 3 AM UTC | PAUSED | On failure |
| **Staging** | 2 AM Toronto | UNPAUSED | On failure |
| **Production** | 1 AM Toronto | UNPAUSED | On failure & success |

## 📊 Pipeline Details

### Job: `medallion_pipeline_job`

**4 Tasks in sequence:**

1. **`bronze_ingestion`**
   - Ingests raw data from source
   - Adds audit metadata
   - Writes to `bronze_customers` table

2. **`silver_transformation`**
   - Depends on: `bronze_ingestion`
   - Cleanses and validates data
   - Deduplicates records
   - Standardizes formats
   - Writes to `silver_customers` table

3. **`gold_aggregation`**
   - Depends on: `silver_transformation`
   - Creates business metrics
   - Calculates KPIs
   - Writes to `gold_customer_summary` and `gold_quality_metrics`

4. **`data_quality_check`**
   - Depends on: `gold_aggregation`
   - Validates all layers
   - Checks record counts
   - Logs validation results

## 📈 Tables Created

### Bronze Layer
- `{catalog}.{schema}.bronze_customers` - Raw customer data with audit metadata

### Silver Layer
- `{catalog}.{schema}.silver_customers` - Cleansed and deduplicated customers

### Gold Layer
- `{catalog}.{schema}.gold_customer_summary` - Customer summary metrics
- `{catalog}.{schema}.gold_quality_metrics` - Data quality metrics

### Data Quality
- `{catalog}.{schema}.dq_validation_log` - Validation results log

## 🎨 Customization

### Adding New Notebooks

1. Create notebook in `src/` directory
2. Add task to `resources/jobs.yml`:
   ```yaml
   - task_key: my_new_task
     depends_on:
       - task_key: previous_task
     notebook_task:
       notebook_path: ../src/my_notebook.py
       base_parameters:
         catalog: ${var.catalog}
         schema: ${var.schema}
   ```

### Changing Data Source

Update `src/bronze_ingestion.py`:
```python
# Example: S3 source
source_path = "s3://my-bucket/data/"

# Example: ADLS source
source_path = "abfss://container@account.dfs.core.windows.net/path/"
```

## 🔍 Monitoring & Troubleshooting

### View Job Runs
```bash
# List recent runs
databricks jobs list-runs --job-id <job-id> --limit 10

# Get run details
databricks jobs get-run --run-id <run-id>
```

### Check Logs
```bash
# View task logs
databricks jobs get-run-output --run-id <run-id>
```

### Common Issues

1. **Permission Errors**
   - Ensure Unity Catalog permissions: `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`
   - Check cluster/warehouse access permissions

2. **Schema Not Found**
   - Verify catalog and schema exist
   - Check variable configuration in `databricks.yml`

3. **Job Fails Immediately**
   - Validate bundle: `databricks bundle validate -t <target>`
   - Check notebook paths are correct

## 📚 Best Practices

✅ **DO:**
- Always test in `dev` before deploying to `staging`/`prod`
- Use meaningful commit messages
- Review job runs and monitor costs
- Set up alerting for production failures
- Document any custom transformations

❌ **DON'T:**
- Deploy directly to production without testing
- Hardcode credentials or secrets
- Skip data quality validations
- Ignore failed data quality checks

## 🔐 Security

- Store credentials in **Databricks Secrets**
- Use **Service Principals** for CI/CD
- Implement **least privilege** access
- Enable **audit logging**
- Review **Unity Catalog** permissions regularly

## 📖 Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake](https://docs.databricks.com/delta/index.html)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📄 License

MIT License - See [LICENSE](LICENSE) file for details

## 👨‍💻 Author

**Pratik Bhikadiya**
- 📧 Email: bhikadiya.pratik@gmail.com
- 🐙 GitHub: [@Pratik2895](https://github.com/Pratik2895)
- 💼 LinkedIn: [Pratik Bhikadiya](https://www.linkedin.com/in/pratik-bhikadiya)
- 🌐 Portfolio: [codebasics.io/portfolio/Bhikadiya-Pratik](https://codebasics.io/portfolio/Bhikadiya-Pratik)

---

⭐ **Star this repo** if you find it helpful!

🐛 **Report issues** on [GitHub Issues](https://github.com/Pratik2895/Databricks-DAB/issues)
