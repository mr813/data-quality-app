#!/bin/bash

echo "🚀 Pushing Data Quality App to GitHub"
echo "====================================="

# Get GitHub username
read -p "Enter your GitHub username: " GITHUB_USERNAME

if [ -z "$GITHUB_USERNAME" ]; then
    echo "❌ Username is required"
    exit 1
fi

# Repository name
REPO_NAME="data-quality-app"
REPO_URL="https://github.com/$GITHUB_USERNAME/$REPO_NAME.git"

echo ""
echo "📋 Repository Details:"
echo "   Username: $GITHUB_USERNAME"
echo "   Repository: $REPO_NAME"
echo "   URL: $REPO_URL"
echo ""

read -p "Have you created the repository on GitHub? (y/n): " CONFIRM

if [[ $CONFIRM =~ ^[Yy]$ ]]; then
    echo ""
    echo "🔗 Setting up remote and pushing..."
    
    # Add remote
    git remote add origin $REPO_URL
    
    # Set main branch
    git branch -M main
    
    # Push to GitHub
    git push -u origin main
    
    echo ""
    echo "✅ Successfully pushed to GitHub!"
    echo "🌐 Your repository is now available at: $REPO_URL"
    echo ""
    echo "🎉 Your Data Quality App is now on GitHub!"
else
    echo ""
    echo "📝 Please create the repository first:"
    echo "1. Go to https://github.com/new"
    echo "2. Repository name: $REPO_NAME"
    echo "3. Description: Data Quality & Observability Platform with automatic Spark fallback for macOS compatibility"
    echo "4. Make it Public or Private"
    echo "5. DO NOT initialize with README, .gitignore, or license"
    echo "6. Click 'Create repository'"
    echo "7. Then run this script again"
fi 