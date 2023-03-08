# BonsaiDb User's Guide

BonsaiDb is an ACID-compliant, document-database written in Rust. Its goal is to be a general-purpose database that aims to simplify development and deployment by providing reliable building blocks that are lightweight enough for hobby projects running with minimal resources, but scalable for when your hobby project becomes a deployed product.

This user's guide aims to provide a guided walkthrough for users to understand how BonsaiDb works. This guide is meant to be supplemental [to the documentation]({{DOCS_BASE_URL}}/bonsaidb/). If you learn best by exploring examples, [many are available in `/examples` in the repository]({{REPO_BASE_URL}}/examples). If, however, you learn best by taking a guided tour of how something works, this guide is specifically for you.

If you have any feedback on this guide, please [file an issue](https://github.com/khonsulabs/bonsaidb/issues), and we will try to address any issues or shortcomings.

Thank you for exploring BonsaiDb.

## About `dev.bonsaidb.io`

The domain that is hosting this user guide is powered by [Dossier][dossier]. Dossier is a static file hosting project that is powered by BonsaiDb's [file storage features][bonsaidb-files], currently served on a [Stardust instance in Amsterdam at Scaleway](https://www.scaleway.com/en/stardust-instances/). Every page/image/script is loaded from BonsaiDb (although the domain has caching by Cloudflare).

[dossier]: https://github.com/khonsulabs/dossier
[bonsaidb-files]: {{DOCS_BASE_URL}}/bonsaidb_files/
