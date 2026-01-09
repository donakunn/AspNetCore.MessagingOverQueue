# Test Execution Context Pattern

## Problem
Previously, test handlers used static fields to track execution state (counters, collected messages, etc.). This caused test failures when multiple integration test files ran in parallel, as they would interfere with each other's state.

## Solution
We've implemented a **Test Execution Context** pattern that provides isolated state for each test execution:

### Architecture

1. **TestExecutionContext**: A container that holds all test-related state (counters, collectors, custom data)
2. **TestExecutionContextAccessor**: Uses `AsyncLocal<T>` to provide thread-safe, async-flow-local access to the context
3. **IsolatedIntegrationTestBase**: Base class that sets up and tears down the context for each test
4. **Updated Handlers**: All test handlers now use the context instead of static fields

### How to Use

#### 1. Inherit from IsolatedIntegrationTestBase

```csharp
public class MyIntegrationTests : IsolatedIntegrationTestBase
{
    [Fact]
    public async Task MyTest()
    {
        // The context is automatically set up
        // Your test code here
        
        // Access handler state as before
        var count = SimpleTestEventHandler.HandleCount;
        var messages = SimpleTestEventHandler.HandledMessages;
    }
}
```

#### 2. The Context is Automatically Managed

- **Setup**: The constructor creates a new `TestExecutionContext` and sets it as the current context
- **Isolation**: Each test instance gets its own context, even when tests run in parallel
- **Cleanup**: The `Dispose` method clears the context

#### 3. Handler State Access Remains the Same

The public API of handlers hasn't changed:

```csharp
// Still works the same way
SimpleTestEventHandler.Reset();
await SimpleTestEventHandler.WaitForCountAsync(5, TimeSpan.FromSeconds(10));
var count = SimpleTestEventHandler.HandleCount;
var messages = SimpleTestEventHandler.HandledMessages;
```

### Benefits

? **Parallel Test Execution**: Tests can run in parallel without interfering with each other
? **Thread-Safe**: `AsyncLocal<T>` ensures context isolation across async boundaries
? **Clean API**: Test code remains unchanged; handlers still have static properties
? **SOLID Compliant**: 
   - Single Responsibility: Each class has one clear purpose
   - Open/Closed: Extensible through custom data
   - Dependency Inversion: Handlers depend on abstraction (context accessor)
? **Maintainable**: Clear separation of concerns between test infrastructure and test logic

### How It Works

```
Test Instance A                    Test Instance B
     ?                                  ?
     ?? Creates TestExecutionContext    ?? Creates TestExecutionContext
     ?  (Context A)                     ?  (Context B)
     ?                                  ?
     ?? Sets AsyncLocal to Context A    ?? Sets AsyncLocal to Context B
     ?                                  ?
     ?? Handler executes ????????????   ?? Handler executes ????????????
     ?                              ?   ?                              ?
     ?  Accesses Context A ??????????   ?  Accesses Context B ??????????
     ?  (isolated state)                ?  (isolated state)
     ?                                  ?
     ?? Disposes Context A              ?? Disposes Context B
```

### Migration Guide

If you have existing tests NOT using `IsolatedIntegrationTestBase`:

1. Change your test class to inherit from `IsolatedIntegrationTestBase`:
   ```csharp
   public class MyTests : IsolatedIntegrationTestBase
   ```

2. If you have a `Dispose` method, call `base.Dispose()`:
   ```csharp
   public override void Dispose()
   {
       // Your cleanup code
       base.Dispose();
   }
   ```

3. That's it! Your tests should now be isolation-ready.

### Troubleshooting

**Error: "TestExecutionContext is not set"**
- Ensure your test class inherits from `IsolatedIntegrationTestBase`
- If using a custom base class, make sure it properly initializes the context

**Handlers still share state**
- Verify all test handlers have been updated to use `TestExecutionContextAccessor`
- Check that you're not creating handlers outside the DI container with static state

### Advanced Usage

#### Custom Data Storage

For handlers that need custom state (like concurrency tracking):

```csharp
var context = TestExecutionContextAccessor.GetRequired();
context.SetCustomData("MyKey", myValue);
var value = context.GetCustomData<int>("MyKey");
```

#### Manual Context Management

If you need fine-grained control:

```csharp
var context = new TestExecutionContext();
TestExecutionContextAccessor.Current = context;

// Your code

TestExecutionContextAccessor.Current = null;
```
