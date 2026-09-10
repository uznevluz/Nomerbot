from aiogram.fsm.state import State, StatesGroup


class TopUp(StatesGroup):
    amount = State()
    photo = State()


class BuyNumber(StatesGroup):
    choosing_country = State()
    confirming = State()


class BuyStars(StatesGroup):
    username = State()
    amount = State()
    confirming = State()


class BuyPremium(StatesGroup):
    months = State()
    username = State()
    confirming = State()
