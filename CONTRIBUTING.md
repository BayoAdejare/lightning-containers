# Contributing to Lightning Containers

Thank you for your interest in contributing to Lightning Containers! This document outlines how you can contribute to the project and help us improve our docker-powered lightning atmospheric dataset analysis tool.

## Table of Contents
1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
3. [How to Contribute](#how-to-contribute)
4. [Development Setup](#development-setup)
5. [Pull Request Process](#pull-request-process)
6. [Coding Standards](#coding-standards)
7. [Testing Guidelines](#testing-guidelines)
8. [Documentation](#documentation)

## Code of Conduct

This project and everyone participating in it are governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/your-username/lightning-containers.git
   cd lightning-containers
   ```
3. Set up your development environment following the instructions in the [Development Setup](#development-setup) section

## How to Contribute

There are many ways to contribute:
- Report bugs and request features by creating [issues](https://github.com/BayoAdejare/lightning-containers/issues)
- Improve documentation
- Submit bug fixes or new features through pull requests
- Help answer questions in discussions

## Development Setup

1. Ensure you have the required resources:
   - Minimum: 2 CPU cores, 6GB RAM, 8GB Storage
   - Recommended: 4+ CPU cores, 16GB RAM, 24GB Storage

2. Set up your environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   pip install -r requirements.txt
   ```

3. To run the project locally:
   ```bash
   prefect server start
   python src/flows.py
   streamlit run app/dashboard.py
   ```

## Pull Request Process

1. Create a new branch for your feature or bugfix:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and commit them:
   ```bash
   git add .
   git commit -m "Description of your changes"
   ```

3. Ensure your code passes all tests:
   ```bash
   pytest
   ```

4. Update documentation if necessary

5. Push your branch and create a pull request

6. Wait for review and address any feedback

## Coding Standards

- Follow PEP 8 guidelines for Python code
- Use type hints where appropriate
- Document all functions and classes using docstrings
- Keep functions focused and modular
- Use meaningful variable and function names

## Testing Guidelines

- Write unit tests for all new functionality
- Ensure all tests pass before submitting a pull request
- Test structure should mirror the source code structure
- Include both positive and negative test cases
- Follow the existing test naming conventions

## Documentation

- Update README.md if you change functionality
- Document complex algorithms or workflows
- Update requirements.txt if you add dependencies
- Include docstrings for all public functions and classes

---

Thank you for contributing to Lightning Containers!
