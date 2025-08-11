#!/bin/bash
set -e

BACKUP_DIR="./backups/$(date +%Y-%m-%d_%H-%M-%S)"
mkdir -p "$BACKUP_DIR"

echo "💾 Creating Autonomous SDLC Backup"

# Backup database
echo "📊 Backing up database..."
docker-compose -f docker-compose.production.yml exec -T postgres pg_dump -U ${POSTGRES_USER:-sdlc_admin} autonomous_sdlc > "$BACKUP_DIR/database.sql"

# Backup configuration
echo "⚙️  Backing up configuration..."
cp -r config/ "$BACKUP_DIR/" 2>/dev/null || echo "No config directory found"
cp .env.production "$BACKUP_DIR/" 2>/dev/null || echo "No .env.production found"

# Backup logs
echo "📝 Backing up logs..."
docker-compose -f docker-compose.production.yml logs > "$BACKUP_DIR/application.log"

echo "✅ Backup completed: $BACKUP_DIR"
echo "📦 Backup size: $(du -sh $BACKUP_DIR | cut -f1)"
