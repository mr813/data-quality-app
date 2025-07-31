#!/bin/bash

# GitHub Repository Setup Script
# This script helps you set up your data quality app on GitHub

echo "🚀 Setting up GitHub repository for Data Quality App"
echo "=================================================="

# Check if git is initialized
if [ ! -d ".git" ]; then
    echo "❌ Git repository not found. Please run 'git init' first."
    exit 1
fi

# Check if we have commits
if ! git rev-parse HEAD >/dev/null 2>&1; then
    echo "❌ No commits found. Please commit your changes first."
    exit 1
fi

echo "✅ Git repository is ready"

# Get repository name from user
read -p "Enter your GitHub username: " GITHUB_USERNAME
read -p "Enter repository name (default: data-quality-app): " REPO_NAME
REPO_NAME=${REPO_NAME:-data-quality-app}

echo ""
echo "📋 Repository Details:"
echo "   Username: $GITHUB_USERNAME"
echo "   Repository: $REPO_NAME"
echo "   URL: https://github.com/$GITHUB_USERNAME/$REPO_NAME"
echo ""

read -p "Do you want to create this repository? (y/n): " CONFIRM

if [[ $CONFIRM =~ ^[Yy]$ ]]; then
    echo ""
    echo "🔗 Setting up remote repository..."
    
    # Add remote
    git remote add origin https://github.com/$GITHUB_USERNAME/$REPO_NAME.git
    
    # Set main branch
    git branch -M main
    
    echo "✅ Remote added successfully"
    echo ""
    echo "📝 Next steps:"
    echo "1. Go to https://github.com/new"
    echo "2. Create repository: $REPO_NAME"
    echo "3. Description: Data Quality & Observability Platform with automatic Spark fallback for macOS compatibility"
    echo "4. Make it Public or Private as you prefer"
    echo "5. DO NOT initialize with README, .gitignore, or license"
    echo "6. Click 'Create repository'"
    echo "7. Then run: git push -u origin main"
    echo ""
    echo "🎉 Your repository will be ready!"
else
    echo "❌ Setup cancelled"
fi 