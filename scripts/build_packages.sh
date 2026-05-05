#!/bin/bash
# Build all packages for release
set -e

cd "$(dirname "$0")/.."

echo "=== Building packages ==="

for pkg in packages/*/; do
    name=$(basename "$pkg")
    echo "Building $name..."
    cd "$pkg"
    
    # Clean previous builds
    rm -rf dist/ build/ *.egg-info/
    
    # Build wheel and sdist
    pip install build -q
    python -m build
    
    echo "$name built successfully:"
    ls -lh dist/
    
    cd - > /dev/null
    echo ""
done

echo "=== All packages built ==="