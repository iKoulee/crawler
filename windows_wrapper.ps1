<#
.SYNOPSIS
    PowerShell wrapper for crawler.py that manages Python installation, virtual environment setup, and execution.

.DESCRIPTION
    This script checks if Python is installed, installs it via winget if needed (with user confirmation),
    creates a Python virtual environment if it doesn't exist, installs required dependencies, 
    and runs crawler.py with the provided arguments.

.PARAMETER args
    Arguments to pass to crawler.py.

.EXAMPLE
    .\windows_wrapper.ps1 harvest -c config.yml -l DEBUG
    
.NOTES
    Author: iKoulee Team
#>

# Script configuration
$VenvDir = Join-Path $PSScriptRoot ".venv"
$RequirementsFile = Join-Path $PSScriptRoot "requirements.txt"
$CrawlerScript = Join-Path $PSScriptRoot "src\crawler.py"
$PythonPackageName = "Python.Python.3.11"
$MinPythonVersion = [version]"3.8"

# Function to check if Python is available and meets minimum version
function Test-PythonAvailable {
    try {
        $pythonVersion = python --version 2>&1
        if ($pythonVersion -match 'Python (\d+\.\d+\.\d+)') {
            $version = [version]$Matches[1]
            if ($version -ge $MinPythonVersion) {
                Write-Host "Found Python $version" -ForegroundColor Green
                return $true
            }
            else {
                Write-Host "Python version $version is too old. Version $MinPythonVersion or newer is required." -ForegroundColor Yellow
                return $false
            }
        }
        return $false
    }
    catch {
        Write-Host "Python is not available in PATH." -ForegroundColor Yellow
        return $false
    }
}

# Function to check if winget is available
function Test-WingetAvailable {
    try {
        $null = winget --version
        return $true
    }
    catch {
        return $false
    }
}

# Function to install Python using winget
function Install-PythonWithWinget {
    if (-not (Test-WingetAvailable)) {
        Write-Host "Winget is not available on this system. Please install Python manually." -ForegroundColor Red
        Write-Host "Download Python from: https://www.python.org/downloads/" -ForegroundColor Cyan
        return $false
    }

    $confirmation = Read-Host "Python $MinPythonVersion or newer is required but not found. Install it now? (y/n)"
    if ($confirmation -ne 'y') {
        Write-Host "Python installation cancelled by user." -ForegroundColor Yellow
        return $false
    }

    Write-Host "Installing Python using winget..." -ForegroundColor Cyan
    winget install --id $PythonPackageName --accept-source-agreements --accept-package-agreements

    # Check if installation was successful
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Failed to install Python. Please install it manually." -ForegroundColor Red
        return $false
    }

    Write-Host "Python installed successfully." -ForegroundColor Green
    Write-Host "Please restart this script to use the newly installed Python." -ForegroundColor Cyan
    exit 0
}

# Function to create and set up virtual environment
function Initialize-VirtualEnvironment {
    if (-not (Test-Path $VenvDir)) {
        Write-Host "Creating virtual environment at $VenvDir..." -ForegroundColor Yellow
        python -m venv $VenvDir

        if (-not $?) {
            Write-Host "Failed to create virtual environment." -ForegroundColor Red
            exit 1
        }
        Write-Host "Virtual environment created successfully." -ForegroundColor Green
    }
    else {
        Write-Host "Using existing virtual environment at $VenvDir" -ForegroundColor Green
    }

    # Activate virtual environment
    $ActivateScript = Join-Path $VenvDir "Scripts\Activate.ps1"
    & $ActivateScript

    # Check if activation was successful
    if (-not $?) {
        Write-Host "Failed to activate virtual environment." -ForegroundColor Red
        exit 1
    }

    # Install dependencies if requirements file exists
    if (Test-Path $RequirementsFile) {
        Write-Host "Installing dependencies from $RequirementsFile..." -ForegroundColor Yellow
        pip install -r $RequirementsFile

        if (-not $?) {
            Write-Host "Failed to install dependencies." -ForegroundColor Red
            exit 1
        }
        Write-Host "Dependencies installed successfully." -ForegroundColor Green
    }
    else {
        Write-Host "Warning: Requirements file not found at $RequirementsFile" -ForegroundColor Yellow
    }
}

# Main execution flow
if (-not (Test-PythonAvailable)) {
    if (-not (Install-PythonWithWinget)) {
        exit 1
    }
}

# Initialize and activate virtual environment
Initialize-VirtualEnvironment

# Check if crawler.py exists
if (-not (Test-Path $CrawlerScript)) {
    Write-Host "Error: Crawler script not found at $CrawlerScript" -ForegroundColor Red
    exit 1
}

# Display command being executed
Write-Host "Running: python $CrawlerScript $args" -ForegroundColor Cyan

# Execute crawler.py with all passed arguments
python $CrawlerScript $args

# Return the exit code from the Python script
exit $LASTEXITCODE
