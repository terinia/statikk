====================
Advanced usage guide
====================

This section demonstrates some advanced usage of the library. This section assumes that you have a fundamental understanding of
the Single Table Application architecture.

====================
Table definition
====================

In a single table application you will have a single table that contains all of your data. This table will have a primary key and
at least one Global Secondary Index for optimal query access patterns. The primary key is usually a UUID or a hash of some sort, while
the GSI is a composite key that allows you to query the table in different ways. Statikk fundamentally relies on this assumption.

Here is an example of how you can define a table.

.. code-block:: python

  from statikk.models Table, GlobalSecondaryIndex, KeySchema

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
    models=[Player, Card],
    delimiter="|" # this is the default delimiter for indexes
  )

By defining one index on the table, Statikk can construct index values dynamically for you. All you need to do is define your models and mark
which field(s) should be part of the index's sort key. Let's take a look at a model definition.

Note that by default an index value is considered to be string. You can override this type and even assign default values to an index field:

``sort_key=Key(name="gsi_sk", type=int, default=0)``

====================
Model definition
====================

.. code-block:: python

  from statikk.models import DatabaseModel, PrimaryKeyField, SecondaryKeyField

  class Player(DatabaseModel):
    # we don't define an id field, so a uuid will be automatically assigned to the model when it's created
    last_login_date: IndexSecondaryKeyField

    @classmethod
    def type_is_primary_key(cls):
        # Makes the model's type the primary key. The model's type is the class name
        return True

    @classmethod
    def include_type_in_sort_key(cls):
        # By default, the model's type is alawys included in the sort key. This is useful to avoid collisions of similar
        # models that share the same primary key. Here however, we want the type to be the primary key only.
        return False


  class Card(DatabaseModel):
    id: str
    player_id: IndexPrimaryKeyField
    tier: IndexSecondaryKeyField
    values: list[int]
    cost: int

The above setup allows us to query the table in the following ways:
 - Get all players
 - Get all players by their last login date
 - Get all cards belonging to a player
 - Get all cards belonging to a player by their tier

When you instantiate a model, depending on its configuration, Statikk will automatically create all index fields defined on the table.
Let's take a look at the following example:

.. code-block:: python

  player = Player(last_login_date=datetime(2023, 9, 24))
  player.save()

  card = Card(player_id=player.id, tier="EPIC", values=[1, 2, 3, 4], cost=5)
  card.save()

Statikk will save the following two documents into your table:

.. code-block:: json

  { # Player object
    "id": "<random uuid>",
    "last_login_date": 1695420000 (datetimes are automatically serialized to timestamps),
    "type": "Player",
    "gsi_pk": "Player",
    "gsi_sk": 1695420000
  },
  { # Card object
    "id": "<random uuid>",
    "player_id": "<player id>",
    "tier": "EPIC",
    "type": "Card",
    "values": [1, 2, 3, 4],
    "cost": 5
    "gsi_pk": "<player_id>",
    "gsi_sk": "Card|EPIC"
  }


====================
Querying
====================

That's great, but how can we query the table? Statikk provides two interfaces for you - one lower level and one higher level for
convienience (in case you want to migrate away from PynamoDb).

Let's take a look at some examples:

.. code-block:: python

  from statikk.conditions import Equals, BeginsWith
  from boto3.dynamodb.conditions import Attr

  def query_data():
    # Get all players
    players = list(table.scan(Player)) # returns a list of Player objects
    # you can achieve the same effect using Player.scan()
    player = players[0]

    # Get all EPIC cards for the player that cost 4 or more.
    cards = list(Card.query(hash_key=Equals(player.id), range_key=BeginsWith("EPIC"), filter_condition=Attr("cost").gte(4)))

    # All the APIs that return multiple elements return a generator by default.
    for card in Card.query(...):
      pass

You might notice a few things here. First of all we start by importing some conditions from both Statikk and boto3. While it made
sense for us to write an abstraction layer on top of boto's Key conditions to make the interface slicker, the same couldn't really be
said for filter conditions. The reason for this is that filter conditions are very complex and there are many different ways to use them,
while Key conditions are very simple and have a very limited set of use cases.

You might also notice that we only specified the card's tier ("EPIC") in our range_key query, but the raw data (gsi_sk) in DynamoDb actually starts
with the classes type ("Card|EPIC"). This is because Statikk automatically prepends the model's type to the index value. This is useful to avoid collisions
in models that share very similar structures. This only happens if you provide a ``BeginsWith`` range key condition to your query. Or if you don't provide
one at all.

====================
More advanced queries
====================

Most times, Single Table Applications rely on their index fields to be constructed of multiple different values. Statikk does this for you
based on all the IndexSecondaryKeyField fields you define on your models. If you have more than 1 IndexSecondaryKeyField, Statikk will produce
the constructed index value based on the order of the fields you define. This is very useful, if you want to set up hierarchical queries on your data.

Let's take a look at an example:

.. code-block:: python

    class MultiKeyCard(DatabaseModel):
      id: str
      player_id: IndexPrimaryKeyField
      origin: IndexSecondaryKeyField
      tier: IndexSecondaryKeyField

This setup allows you to search using the following patterns:
 - Get all cards belonging to a player
 - Get all cards belonging to a player by their origin
 - Get all cards belonging to a player by their origin and tier

Note that the order is **REALLY** important here. Swapping up the order in on your production data will cause absolute havoc on your queries
and will taint your data. So be careful! Statikk will not prevent you from doing this, because it's not possible to do so.

There is some work planned to make this a bit more robust, but for now, you'll have to pay attention.

====================
Multiple indexes
====================

Statikk also supports multiple indexes. This is useful if you want to query your data in different ways. Let's take a look at an example:

.. code-block:: python

  class MultiIndexModel(DatabaseModel):
    player_id: IndexPrimaryKeyField
    tier: IndexSecondaryKeyField
    origin: IndexPrimaryKeyField = IndexPrimaryKeyField(index_names=["secondary-index"])
    unit_class: IndexSecondaryKeyField = IndexSecondaryKeyField(index_names=["main-index", "secondary-index"])

This setup requires the table to have two indexes defined: main-index and secondary index. Notice that the ``unit_class`` field
is actually part of both the main and the secondary index. So when Statikk constructs the index values for this model, it will include
``unit_class`` as the last piece of both indexes. This allows you to query the table in the following ways:

 - Get all models by their player_id
 - Get all models by their player_id and tier
 - Get all models by their player_id and origin
 - Get all models by their player_id, tier and origin
 - Get all models by their player_id and unit_class
 - Get all models by their player_id, tier and unit_class
 - Get all models by their player_id, origin and unit_class
 - Get all models by their player_id, tier, origin and unit_class

====================
Index typing
====================

So far we have only looked at string-based indexes. Statikk enforces that the type of the Index fields on your models match
the index definition you defined on the table. This is also a DynamoDB restriction; while DyanmoDB is schemalass, you can't mix
and match different types for attribute properties (keys, indexes, etc).

Using numeric types, for example, means you'll lose out on the hierarchical search capabilities, but will let you query your data
based on more conditions.

For example:

.. code-block:: python

    class Card(DatabaseModel):
        player_id: IndexPrimaryKeyField
        cost: IndexSecondaryKeyField(type=int)

    def query_data():
       models = list(Card.query(hash_key=Equals(player.id), range_key=GreaterThan(4), filter_condition=Attr("type").gte("Card")))

**Important**: If you use numeric types, there is no way to rely on Statikk to prefix your secondary keys with the type of
your models (since the type is not a string), so to avoid collisions where multiple models rely on this structure, make sure
to include a filter condition in your queries!

====================
Batch write
====================

Statikk also supports batch writes. This is useful if you want to write multiple models at once. Statikk will take care of all the
data buffering for you and will write the data in batches of 25 items. Let's take a look at an example:

.. code-block:: python

    with MyAwesomeModel.batch_write() as batch:
        for i in range(50):
            model = MyAwesomeModel(id=f"foo_{i}", player_id="123", tier="LEGENDARY")
            batch.put(model)

Statikk will make two requests to DynamoDb with two batches of 25 items.

====================
Batch get
====================

Similarly, Statikk also supports batch get requests on tables. This is a great way to reduce roundtrips to the Database when
you need to fetch multiple models at once. Let's take a look at an example:

.. code-block:: python

    card_ids = ["card-1", "card-2", "card-3", "card-4"]
    models = list(Card.batch_get_items(card_ids))

Again, Statikk will handle all the buffering for you as DynamoDb has some limitations on not only the amount of documents that
can be returned in a single batch, but also on the size of that data.

``batch_get_items`` also returns a generator, so you can iterate over the results as they come in.
