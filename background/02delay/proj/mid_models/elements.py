class WindExtremum:
    """
        风要素极值
    """

    def __init__(self, val: float, dir: float, ts: int):
        """

        @param val:
        @param dir:
        @param ts:
        """
        self.val = val
        """风要素极值"""
        self.dir = dir
        """极值风向"""
        self.ts = ts
        """极值出现时间"""
