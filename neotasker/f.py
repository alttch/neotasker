import traceback


class FunctionCollection:
    """
    Args:
        on_error: function, launched when function in collection raises an
                    exception
        on_error_kwargs: additional kwargs for on_error function
        include_exceptions: include exceptions into final result dict
    """

    def __init__(self, **kwargs):
        self._functions = []
        self._functions_with_priorities = []
        self.on_error = kwargs.get('on_error')
        self.on_error_kwargs = kwargs.get('on_error_kwargs', {})
        self.include_exceptions = True if kwargs.get(
            'include_exceptions') else False
        self.default_priority = 100

    def __call__(self, f=None, **kwargs):

        def wrapper(f, **kw):
            self.append(f, **kwargs)

        if f:
            self.append(f)
            return f
        elif kwargs:
            return wrapper
        else:
            return self.run()

    def append(self, f, priority=None):
        """
        Append function without annotation

        Args:
            f: function
            priority: function priority
        """
        if f not in self._functions:
            self._functions.append(f)
            self._functions_with_priorities.append({
                'p': priority if priority else self.default_priority,
                'f': f
            })

    def remove(self, f):
        """
        Remove function

        Args:
            f: function
        """
        try:
            self._functions.remove(f)
            for z in self._functions_with_priorities:
                if z['f'] is f:
                    self._functions_with_priorities.remove(z)
                    break
        except:
            self.error()

    def run(self):
        """
        Run all functions in collection

        Returns:
            result dict as
            
            { '<function>': '<function_return>', ... }
        """
        return self.execute()[0]

    def execute(self):
        """
        Run all functions in collection

        Returns:
            a tuple
            { '<function>': '<function_return>', ...}, ALL_OK
            where ALL_OK is True if no function raised an exception
        """
        result = {}
        all_ok = True
        funclist = sorted(self._functions_with_priorities, key=lambda k: k['p'])
        for fn in funclist:
            f = fn['f']
            k = '{}.{}'.format(f.__module__, f.__name__)
            try:
                result[k] = f()
            except Exception as e:
                if self.include_exceptions:
                    result[k] = (e, traceback.format_exc())
                else:
                    result[k] = None
                self.error()
                all_ok = False
        return result, all_ok

    def error(self):
        if self.on_error:
            self.on_error(**self.on_error_kwargs)
        else:
            raise
