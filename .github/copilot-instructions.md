# Coding Standards and Development Guidelines

This document outlines the coding standards and development practices to be followed when contributing to this project. These guidelines aim to maintain code quality, consistency, and testability across the codebase.

## Python Standards

### PEP 8 Compliance

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide for Python code.
- Use 4 spaces for indentation (no tabs).
- Keep lines to a maximum of 79 characters for code and 72 for comments and docstrings.
- Use appropriate naming conventions:
  - `snake_case` for variables, functions, methods, and modules
  - `PascalCase` for classes
  - `SCREAMING_SNAKE_CASE` for constants
- Use meaningful and descriptive names.
- Add appropriate whitespace around operators and after commas.
- Use consistent import ordering:
  1. Standard library imports
  2. Related third-party imports
  3. Local application/library specific imports
- Separate import groups with a blank line.

### Type Hints

- Always include type hints for function parameters and return values following [PEP 484](https://www.python.org/dev/peps/pep-0484/).
- Use the typing module for complex types:
  ```python
  from typing import List, Dict, Tuple, Optional, Union, Any, Callable
  ```
- For function annotations:
  ```python
  def process_data(input_data: List[Dict[str, Any]]) -> Dict[str, float]:
      # Function body
      return result
  ```
- For variable annotations:
  ```python
  names: List[str] = ["Alice", "Bob"]
  age_map: Dict[str, int] = {"Alice": 30, "Bob": 25}
  ```
- Use Optional for parameters that could be None:
  ```python
  def process_user(user_id: Optional[int] = None) -> None:
      # Function body
      pass
  ```
- For class variables, use annotations:
  ```python
  class User:
      name: str
      age: int
      is_active: bool = True
  ```

## Standards for Other Languages

### JavaScript/TypeScript

- Use camelCase for variables, functions, and methods.
- Use PascalCase for classes and component names.
- Use 2-space indentation.
- End statements with semicolons.
- Use ES6+ features when available.
- Use JSDoc comments for documentation.
- For TypeScript, use explicit type annotations.

### HTML/CSS

- Use 2-space indentation.
- Use lowercase for HTML elements and attributes.
- Use kebab-case for CSS class names.
- Follow BEM (Block Element Modifier) methodology for CSS classes when applicable.

### SQL

- Use UPPERCASE for SQL keywords.
- Use snake_case for table and column names.
- Indent SQL statements for readability.
- Use meaningful table and column names.

## Object-Oriented Programming Best Practices

### General OOP Principles

- **Encapsulation**: Hide implementation details and expose only necessary interfaces. Use private attributes with appropriate getters and setters when needed.
- **Inheritance**: Use inheritance when there is a true "is-a" relationship. Prefer composition over inheritance when possible.
- **Polymorphism**: Design flexible interfaces that allow objects of different types to be treated uniformly.
- **Abstraction**: Create abstractions that hide complexity and expose only what's necessary.

### Class Design

- Create cohesive classes with a single, well-defined responsibility.
- Keep class interfaces simple and intuitive, using consistent naming patterns.
- Design classes to be open for extension but closed for modification (Open/Closed Principle).
- Implement dependency injection to reduce tight coupling between classes.
- Use abstract classes and interfaces to define contracts and enable polymorphism.
- Make base classes abstract when they represent concepts rather than concrete objects.

### Object Relationships

- Prefer composition over inheritance when objects have a "has-a" relationship rather than an "is-a" relationship.
- Use interfaces to define behavior contracts that multiple unrelated classes can implement.
- Apply the Liskov Substitution Principle: subtypes should be substitutable for their base types without altering program correctness.
- Avoid deep inheritance hierarchies (typically no more than 2-3 levels deep).
- Use delegation when behavior should be shared but an "is-a" relationship is inappropriate.

## Clean Code Principles (Kent Beck Style)

### General Principles

- **Simplicity**: Write the simplest code that works.
- **Clarity**: Code should be readable and intention-revealing.
- **DRY (Don't Repeat Yourself)**: Eliminate duplication.
- **YAGNI (You Aren't Gonna Need It)**: Don't implement features until they are needed.
- **Small Functions/Methods**: Keep functions and methods small and focused.
- **Single Responsibility Principle**: Each function, class, or module should have one responsibility.
- **Meaningful Names**: Choose descriptive and intention-revealing names.
- **Comments**: Use comments only when necessary, prefer self-documenting code.
- **Error Handling**: Handle errors gracefully and appropriately.

### Test-Driven Development (TDD)

Follow Kent Beck's rigorous TDD approach:

1. **Red**: Write a failing test that defines the functionality you want to implement.
2. **Green**: Write the simplest code that makes the test pass.
3. **Refactor**: Clean up the code while keeping the tests passing.

#### TDD Practices

- Write the test before writing the implementation code.
- Start with the simplest test case.
- Add complexity gradually with additional tests.
- Maintain a fast test cycle (write test, see it fail, make it pass, refactor).
- Test one behavior at a time.
- Keep tests independent of each other.
- Focus on behavior, not implementation details.
- Run tests frequently.

## Code Size and Complexity Guidelines

### Change Size Limitations

- **Incremental Changes**: Any single code change should not exceed 50 lines. This includes:
  - New functions or methods
  - Modifications to existing code
  - Class definitions
  - Configuration updates
  
- **Review Process**: After implementing a change within this 50-line limit:
  - Submit for review
  - Wait for approval before proceeding with related changes
  - This ensures that changes are small, focused, and reviewable

- **Exceptions**: In rare cases where a larger change is unavoidable:
  - Document the reasoning
  - Consider breaking the change into smaller logical parts
  - Discuss with the team before proceeding

### Function and Method Size Constraints

- **General Limit**: Functions, methods, and routines should not exceed 20 lines of code.
  
- **Benefits of Short Functions**:
  - Improved readability and comprehension
  - Easier testing and debugging
  - Better function naming that describes a single purpose
  - Simplified maintenance
  - More opportunities for code reuse

- **Acceptable Exceptions**:
  - Data transformation routines with consistent logic
  - State machines with clear, repetitive patterns
  - Generated code
  - Cases where breaking the function would reduce clarity rather than improve it
  
- **Implementation Strategy**:
  - Extract repeated code into helper functions
  - Use the Single Responsibility Principle to identify when functions are doing too much
  - Name functions precisely to reflect their specific purpose
  - Consider functional programming patterns to reduce function size

- **Documentation Requirements**: When a function necessarily exceeds 20 lines:
  - Add detailed comments explaining why the function cannot be further broken down
  - Clearly document the function's sections and flow
  - Consider adding diagrams or additional documentation for complex functions

## Implementation Guidelines

### Only Write Code Needed to Pass Tests

- Implement only the functionality required to pass the current tests.
- Do not add speculative features or "nice-to-have" functionality without corresponding tests.
- Focus on making the current tests pass before moving on to new functionality.
- If a requirement changes, first update the tests, then update the implementation.

### Specific Implementation Practices

- Start with the simplest implementation that could possibly work.
- Incrementally improve the implementation as more tests are added.
- Refactor code regularly to maintain clean design.
- When refactoring, ensure all tests continue to pass.

## Documentation

- Document public APIs with clear descriptions of parameters, return values, and exceptions.
- Include examples in documentation where appropriate.
- Keep documentation up-to-date with code changes.
- Document design decisions and architectural patterns used.

## Version Control Practices

- Write descriptive commit messages that explain why changes were made.
- Keep commits focused on a single logical change.
- Reference relevant issues or tickets in commit messages.
- Create feature branches for new work.
- Pull and rebase frequently to minimize merge conflicts.

## Conclusion

These guidelines aim to maintain a high-quality, consistent, and testable codebase. By following these standards, we ensure that our code is maintainable, extensible, and reliable. Remember that the ultimate goal is to produce working software that meets the requirements while remaining adaptable to future changes.
