builder.router {

    directory(uri: "/static", root: "module:/static/")


    jaxRs(uri: "", passThrough: true) {
        jaxRsResource(scanPackages: "*")
        jaxRsProvider(scanPackages: "*")
    }

    pages(
        uri: "",
        root: "pages"
    )
    
}