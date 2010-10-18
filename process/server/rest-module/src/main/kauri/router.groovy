builder.router {
    jaxRs(uri: "") {
        jaxRsResource(scanPackages: ["org.lilycms.rest"])
        jaxRsProvider(scanPackages: ["org.lilycms.rest.providers.json"])
    }
}