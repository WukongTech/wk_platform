import wk_util.logger

if wk_util.logger.SHOW_RUNTIME_INFO:
    from tqdm.auto import tqdm
else:
    def tqdm(iterator, disable):
        return iterator

