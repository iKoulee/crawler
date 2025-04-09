# Job Advertisement Crawler and Processor

A robust web crawler specialized for harvesting job advertisements from career portals, analyzing their content for specific keywords, and exporting the results in various formats.

## Overview

This project automates the process of collecting job advertisements from supported job portals, analyzing their content using configurable keyword patterns, and exporting the results for further analysis. The system is built with a focus on extensibility, allowing for easy addition of new job portals and flexible output options.

## Features

- **Multi-portal Support**: Built-in support for StepStone and Karriere.at with an extensible architecture for adding more portals
- **Respectful Crawling**: Follows robots.txt rules and implements configurable request rate limits
- **Configurable Keyword Matching**: Define keywords to search for in job descriptions with support for regular expressions and case sensitivity
- **Flexible Export Options**:
  - CSV export for tabular data analysis
  - Structured HTML file export with customizable nested directory organization
- **Content Filtering**: Filter and categorize advertisements by content using regular expression patterns
- **Persistent Storage**: SQLite database for storing and querying all harvested advertisements
- **Cross-platform Compatibility**: Works on Windows, macOS, and Linux

## Requirements

- Python 3.8 or newer
- Required packages:
  - beautifulsoup4>=4.11.0
  - protego>=0.2.1
  - pyyaml>=6.0
  - requests>=2.28.0

## Installation

### Windows

#### Option 1: Using the PowerShell Wrapper Script (Recommended)

The project includes a PowerShell wrapper script that automates the setup process:

1. Open PowerShell and navigate to the project directory:
   ```powershell
   cd path\to\crawler
   ```

2. Run the wrapper script:
   ```powershell
   .\windows_wrapper.ps1 [command] [options]
   ```

The wrapper script will:
- Check if Python is installed and install it via winget if needed (with your permission)
- Create a virtual environment if one doesn't exist
- Install required dependencies
- Run the crawler with your specified arguments

#### Option 2: Manual Setup

1. Install Python 3.8 or newer from [python.org](https://www.python.org/downloads/)
2. Clone or download this repository
3. Open Command Prompt and navigate to the project directory:
   ```cmd
   cd path\to\crawler
   ```
4. Create a virtual environment:
   ```cmd
   python -m venv .venv
   ```
5. Activate the virtual environment:
   ```cmd
   .venv\Scripts\activate
   ```
6. Install required packages:
   ```cmd
   pip install -r requirements.txt
   ```
7. Run the crawler:
   ```cmd
   python src\crawler.py [command] [options]
   ```

### macOS/Linux

1. Ensure Python 3.8+ is installed:
   ```bash
   # Check Python version
   python3 --version
   ```

   If Python is not installed:
   ```bash
   # macOS (using Homebrew)
   brew install python3

   # Ubuntu/Debian
   sudo apt update
   sudo apt install python3 python3-pip python3-venv
   ```

2. Clone or download this repository:
   ```bash
   git clone https://github.com/iKoulee/crawler.git
   cd crawler
   ```

3. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

4. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

5. Run the crawler:
   ```bash
   python src/crawler.py [command] [options]
   ```

## Configuration

The crawler requires configuration files to define job portals to harvest and keywords to search for.

### Main Configuration (config.yml)

This file defines the job portals to harvest and keywords to search for:

```yaml
# Example configuration
portals:
  - engine: StepStoneHarvester
    url: https://www.stepstone.at
    requests_per_minute: 10
  - engine: KarriereHarvester
    url: https://www.karriere.at
    requests_per_minute: 15

keywords:
  - title: Python Developer
    search: 'python\s+developer'
    case_sensitive: false
  - title: Machine Learning
    search: 'machine\s+learning|ml\s+engineer'
    case_sensitive: false
  - title: JavaScript
    search: 'javascript|react|vue|angular'
    case_sensitive: false
```

### Export Filter Configuration (export_filters.yml)

This file defines filters used to organize exported HTML files into a nested directory structure:

```yaml
filters:
  # Education level category
  education_level:
    higher_education:
      pattern: 'university|college|bachelor|master|phd|degree'
      case_sensitive: false
      description: "Higher education positions"
    vocational:
      pattern: 'vocational|apprentice|trainee|ausbildung'
      case_sensitive: false
      description: "Vocational training positions"
    other_education:
      pattern: '.*'
      catch_all: true
      description: "Other education level positions"
  
  # Job type category
  job_type:
    full_time:
      pattern: 'full[ -]time|vollzeit|permanent'
      case_sensitive: false
      description: "Full-time positions"
    part_time:
      pattern: 'part[ -]time|teilzeit'
      case_sensitive: false
      description: "Part-time positions"
    other_job_type:
      pattern: '.*'
      catch_all: true
      description: "Other job types"
```

## Usage

The crawler has three main commands:

### 1. Harvesting Job Advertisements

Harvest job advertisements from configured portals:

```bash
python src/crawler.py harvest -d advertisements.db
```

This command will fetch job advertisements from the portals defined in `config.yml` and store them in the SQLite database.

### 2. Analyzing Job Advertisements

Analyze the harvested job advertisements for keyword matches:

```bash
python src/crawler.py -l INFO assembly -c etc/config.yml -d advertisements.db -o advertisements.csv
```

This command will process the stored job advertisements and identify matches based on the keywords defined in `config.yml`.

### 3. Exporting Results

Export the analyzed results to the desired format:

```bash
python src/crawler.py =l INFO export -c etc/config.yml -d advertisements.db -o output_dir
```

This command will organize the exported HTML files into a nested directory structure based on the filters defined in `export_filters.yml`.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a new branch for your feature or bugfix
3. Commit your changes with clear and concise messages
4. Submit a pull request

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.


