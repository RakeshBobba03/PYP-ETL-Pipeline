# Environment Setup Guide for PYP ETL Pipeline

## 🚀 **Quick Start**

1. **Create a `.env` file** in your project root directory
2. **Copy the configuration below** into your `.env` file
3. **Update the values** according to your environment
4. **Restart your application** to load the new environment variables

## 📁 **Create Your .env File**

Create a new file called `.env` in your project root directory (same level as `run.py`):

```bash
# Windows
notepad .env

# macOS/Linux
nano .env
# or
vim .env
```

## 🔧 **Environment Configuration**

Copy this configuration into your `.env` file:

```env
# =============================================================================
# PYP ETL Pipeline Environment Configuration
# =============================================================================

# =============================================================================
# FLASK APPLICATION SETTINGS
# =============================================================================
FLASK_SECRET_KEY=your-super-secret-key-change-this-in-production
FLASK_DEBUG=True
FLASK_ENV=development

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================
DB_HOST=localhost
DB_PORT=5432
DB_NAME=pyp_etl_db
DB_USER=pyp_user
DB_PASSWORD=your_secure_password_here

# Alternative: Use a complete DATABASE_URL
# DATABASE_URL=postgresql://username:password@host:port/database_name

# =============================================================================
# DGRAPH CONFIGURATION
# =============================================================================
DGRAPH_URL=http://localhost:8080/graphql
DGRAPH_API_TOKEN=your_dgraph_api_token_here

# Dgraph request settings
DGRAPH_TIMEOUT=30
DGRAPH_MAX_RETRIES=3
DGRAPH_RETRY_DELAY=1

# =============================================================================
# ETL PROCESSING CONFIGURATION
# =============================================================================
FUZZY_MATCH_THRESHOLD=80.0
AUTO_RESOLVE_THRESHOLD=95.0
BATCH_SIZE=1000

# =============================================================================
# FILE UPLOAD SETTINGS
# =============================================================================
MAX_CONTENT_LENGTH=16777216
UPLOAD_FOLDER=seed_data/new_submissions

# =============================================================================
# SECURITY SETTINGS
# =============================================================================
SESSION_COOKIE_SECURE=False
SESSION_COOKIE_HTTPONLY=True
SESSION_COOKIE_SAMESITE=Lax
PERMANENT_SESSION_LIFETIME=24

# =============================================================================
# LOGGING AND MONITORING
# =============================================================================
LOG_LEVEL=INFO
ENABLE_DETAILED_LOGGING=True

# =============================================================================
# PERFORMANCE SETTINGS
# =============================================================================
ENABLE_DB_POOLING=True
DB_POOL_SIZE=10
DB_POOL_TIMEOUT=20

# =============================================================================
# DEVELOPMENT SETTINGS
# =============================================================================
DEBUG=True
TESTING=False
ENABLE_DEV_FEATURES=True
```

## 🔑 **Required Configuration Values**

### **Critical Settings (Must Configure)**

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `FLASK_SECRET_KEY` | Secret key for Flask sessions | Generate with: `python -c "import secrets; print(secrets.token_hex(32))"` |
| `DB_HOST` | PostgreSQL database host | `localhost` or `db` (Docker) |
| `DB_NAME` | Database name | `pyp_etl_db` |
| `DB_USER` | Database username | `pyp_user` |
| `DB_PASSWORD` | Database password | `your_secure_password` |
| `DGRAPH_URL` | Dgraph GraphQL endpoint | `http://localhost:8080/graphql` |
| `DGRAPH_API_TOKEN` | Dgraph authentication token | `your_dgraph_api_token` |

### **Optional Settings (Have Defaults)**

| Variable | Default Value | Description |
|----------|---------------|-------------|
| `DB_PORT` | `5432` | PostgreSQL port |
| `FUZZY_MATCH_THRESHOLD` | `80.0` | Fuzzy matching confidence (0-100) |
| `AUTO_RESOLVE_THRESHOLD` | `95.0` | Auto-approval threshold (0-100) |
| `BATCH_SIZE` | `1000` | Database batch operations size |
| `DGRAPH_TIMEOUT` | `30` | Dgraph request timeout (seconds) |

## 🐳 **Docker Compose Configuration**

If you're using Docker Compose, use these values:

```env
DB_HOST=db
DB_PORT=5432
DB_NAME=flask_db
DB_USER=flask_user
DB_PASSWORD=flask_password
DGRAPH_URL=http://dgraph:8080/graphql
```

## 🔒 **Security Best Practices**

### **Development Environment**
- Use simple passwords for local development
- Keep `FLASK_DEBUG=True` for debugging
- Use `SESSION_COOKIE_SECURE=False`

### **Production Environment**
- Generate a strong `FLASK_SECRET_KEY`
- Use complex, unique passwords
- Set `FLASK_DEBUG=False`
- Set `SESSION_COOKIE_SECURE=True` (requires HTTPS)
- Use environment-specific database credentials

## 🚨 **Important Security Notes**

1. **Never commit `.env` files** to version control
2. **Keep API tokens secure** and rotate them regularly
3. **Use different credentials** for dev/staging/production
4. **Consider using a secrets manager** in production
5. **Regularly audit** your environment variables

## 🔍 **Verification Steps**

After creating your `.env` file:

1. **Check file location**: Ensure `.env` is in the project root
2. **Verify syntax**: No spaces around `=` signs
3. **Test database connection**: Run the application
4. **Check logs**: Look for configuration-related errors

## 🆘 **Troubleshooting**

### **Common Issues**

| Problem | Solution |
|---------|----------|
| "Environment variable not found" | Check `.env` file location and syntax |
| "Database connection failed" | Verify DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD |
| "Dgraph connection failed" | Check DGRAPH_URL and DGRAPH_API_TOKEN |
| "Flask secret key error" | Generate a new FLASK_SECRET_KEY |

### **Debug Commands**

```bash
# Check if .env file exists
ls -la .env

# Verify environment variables are loaded
python -c "import os; print('DB_HOST:', os.getenv('DB_HOST'))"

# Test database connection
python -c "import psycopg2; print('PostgreSQL available')"
```

## 📚 **Additional Resources**

- [Flask Configuration Documentation](https://flask.palletsprojects.com/en/2.3.x/config/)
- [PostgreSQL Connection Strings](https://www.postgresql.org/docs/current/libpq-connect.html)
- [Dgraph Authentication](https://dgraph.io/docs/graphql/admin/auth/)
- [Environment Variables Best Practices](https://12factor.net/config)

## 📞 **Need Help?**

If you encounter issues:

1. Check the troubleshooting section above
2. Verify all required variables are set
3. Check application logs for specific error messages
4. Ensure your database and Dgraph services are running

---

**Last Updated**: December 2024  
**Version**: 1.0
