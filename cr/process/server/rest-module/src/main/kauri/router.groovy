builder.router {
    jaxRs(uri: "") {
        jaxRsResource(scanPackages: ["org.lilyproject.rest"])
        jaxRsProvider(scanPackages: ["org.lilyproject.rest.providers.json"])
    }
}