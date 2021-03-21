from load_ac_shopping_crm import LoadAcShoppingCrm


def execute():
    task = LoadAcShoppingCrm("ac_shopping_crm")
    task.run()


if __name__ == "__main__":
    execute()
