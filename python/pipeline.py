class ETLPipeline:
    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

    def run(self, vector_store):
        data = self.extractor.extract()
        cleaned = self.transformer.transform(data)
        self.loader.load(cleaned, vector_store)
