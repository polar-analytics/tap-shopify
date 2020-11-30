import logging
import shopify

from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream,
                                      RESULTS_PER_PAGE,
                                      shopify_error_handling,
                                      OutOfOrderIdsError)


class InventoryItems(Stream):
    name = 'inventory_items'
    replication_object = shopify.InventoryItem
    replication_key = 'created_at'

    @shopify_error_handling
    def get_inventory_items(self, variant, since_id):
        return self.replication_object.find(
            ids=str(variant.inventory_item_id),
            limit=RESULTS_PER_PAGE,
            since_id=since_id,
            order='id asc')

    def get_objects(self):
        selected_parent = Context.stream_objects['products']()
        selected_parent.name = "inventory_items"

        # Page through all `products` and product variants, bookmarking at `inventory_items`
        for parent_object in selected_parent.get_objects():
            for variant in parent_object.variants:
                since_id = 1
                while True:
                    inventory_items = self.get_inventory_items(variant, since_id)
                    for inventory_item in inventory_items:
                        if inventory_item.id < since_id:
                            raise OutOfOrderIdsError("inventory_item.id < since_id: {} < {}".format(
                                inventory_item.id, since_id))
                        yield inventory_item
                    if len(inventory_items) < RESULTS_PER_PAGE:
                        break
                    if inventory_items[-1].id != max([o.id for o in inventory_items]):
                        raise OutOfOrderIdsError("{} is not the max id in inventory_items ({})".format(
                            inventory_items[-1].id, max([o.id for o in inventory_items])))
                    since_id = inventory_items[-1].id

    def sync(self):
        for inventory_item in self.get_objects():
            inventory_item_dict = inventory_item.to_dict()
            yield inventory_item_dict

Context.stream_objects['inventory_items'] = InventoryItems
