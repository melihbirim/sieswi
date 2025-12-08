# Installation Guide

## Quick Install

### macOS (Homebrew)

```bash
# Coming soon
brew tap sieswi/tap
brew install sieswi
```

### Linux / macOS (curl)

```bash
# Download latest release
curl -L https://github.com/melihbirim/sieswi/releases/latest/download/sieswi_$(uname -s)_$(uname -m).tar.gz | tar xz
sudo mv sieswi /usr/local/bin/
```

### Windows (PowerShell)

```powershell
# Download from releases page
Invoke-WebRequest -Uri "https://github.com/melihbirim/sieswi/releases/latest/download/sieswi_Windows_x86_64.zip" -OutFile "sieswi.zip"
Expand-Archive sieswi.zip
Move-Item sieswi\sieswi.exe C:\Windows\System32\
```

### Go Install

```bash
go install github.com/melihbirim/sieswi/cmd/sieswi@latest
```

## Build from Source

### Prerequisites

- Go 1.22 or later
- Make (optional, for convenience)

### Steps

```bash
# Clone repository
git clone https://github.com/melihbirim/sieswi.git
cd sieswi

# Build with Make
make build

# Or build manually
go build -o sieswi ./cmd/sieswi

# Install to system
make install
# Or manually
cp sieswi /usr/local/bin/
```

### Build with Version Info

```bash
VERSION=v1.0.0 COMMIT=$(git rev-parse --short HEAD) DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ") \
go build -ldflags "-s -w -X main.version=$VERSION -X main.commit=$COMMIT -X main.date=$DATE" \
-o sieswi ./cmd/sieswi
```

## Verify Installation

```bash
# Check version
sieswi --version

# Run test query
echo "name,age
Alice,30
Bob,25" | sieswi "SELECT * FROM '-' WHERE age > 26"
```

Expected output:

```bash
name,age
Alice,30
```

## Docker

```bash
# Build image
docker build -t sieswi .

# Run query
docker run --rm -v $(pwd):/data sieswi "SELECT * FROM '/data/file.csv' WHERE col = 'value'"
```

## Uninstall

### Homebrew

```bash
brew uninstall sieswi
```

### Manual

```bash
rm /usr/local/bin/sieswi  # or wherever you installed it
rm $(which sieswi)        # find and remove
```

## Troubleshooting

### "command not found"

Make sure the installation directory is in your PATH:

```bash
# Check PATH
echo $PATH

# Add to PATH (bash/zsh)
echo 'export PATH=$PATH:/usr/local/bin' >> ~/.bashrc
source ~/.bashrc
```

### Permission denied

```bash
# Make executable
chmod +x sieswi

# Or use sudo for system install
sudo mv sieswi /usr/local/bin/
```

### Go install fails

```bash
# Update Go
go version  # Should be 1.22+

# Clear Go cache
go clean -modcache
go install github.com/melihbirim/sieswi/cmd/sieswi@latest
```

## Next Steps

- Read the [Quick Start Guide](README.md#quick-start)
- Check [SQL Support](README.md#sql-support-phase-1---baseline)
- Learn about [Performance](PARALLEL_PROCESSING.md)
- See [Examples](docs/examples.md)
