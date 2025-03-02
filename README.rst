.. image:: ./assets/logo.png
    :alt: Statikk
    :align: center

An ORM-like Single Table Application (STA) architecture library for Python and DynamoDb. Makes it really eason to set up and work with STAs.

The library is in alpha. Constant work is being done on it as I'm developing my game.

=================
Requirements
=================

- Pydantic 2.3+
- Python 3.8+

Probably works on older versions of Python, but we haven't tested it.

=================
Installation
=================

``pip install statikk``


=================
Basic Usage
=================

.. code-block:: python

    from statikk.models import DatabaseModel, Table, GlobalSecondaryIndex, KeySchema

    class MyAwesomeModel(DatabaseModel):
      player_id: str
      tier: str
      name: str = "Foo"
      values: set = {1, 2, 3, 4}
      cost: int = 4

      @classmethod
      def index_definitions(cls) -> dict[str, IndexFieldConfig]:
        return {"main-index": IndexFieldConfig(pk_fields=["player_id"], sk_fields=["tier"])}

    table = Table(
      name="my-dynamodb-table",
      key_schema=KeySchema(hash_key="id"),
      indexes=[
        GSI(
          name="main-index",
          hash_key=Key(name="gsi_pk"),
          sort_key=Key(name="gsi_sk"),
         )
       ],
      models=[MyAwesomeModel],
    )

    def main():
      my_model = MyAwesomeModel(id="foo", player_id="123", tier="gold")
      my_model.save()
      MyAwesomeModel.update("foo").set(name="Bar").delete("values", {1}).add("cost", 1).execute() # Update multiple fields at once
      my_model = table.get("foo", MyAwesomeModel) # Get a model by its primary key using the table
      my_model = MyAwesomeModel.get("foo") # Get a model by its primary key using the model's class
      my_model.gsi_pk # returns "123"
      my_model.gsi_sk # returns "MyAwesomeModel|gold"
      my_model.delete() # Delete a model

See `the usage docs for more <https://github.com/terinia/statikk/blob/main/docs/usage.rst>`_

=================
Features
=================

- `Single Table Application architecture <https://www.youtube.com/watch?v=HaEPXoXVf2k>`_
- Easy model definition using Pydantic
- Automatic index value construction based on marked fields.
- Get, Update, Delete, Batch Get, Batch Write, Query, and Scan operations
