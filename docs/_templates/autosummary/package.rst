{{ name | escape | underline }}

.. automodule:: {{ fullname }}
   :special-members:
   :members:

   .. autopackagesummary:: {{ fullname }}
      :toctree: .
      :template: autosummary/package.rst
      :caption: Table of Contents
      :recursive:
