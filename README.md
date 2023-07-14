&nbsp;

<div align="center">

<img src="https://raw.githubusercontent.com/tylern4/covalent-nersc-plugin/develop/assets/nersc_readme_banner.png" width=150%>

</div>

## Covalent NERSC Plugin

Covalent is a Pythonic workflow tool used to execute tasks on advanced computing hardware.

This executor plugin interfaces Covalent with the [NERSC Superfacility](https://www.nersc.gov/research-and-development/superfacility/) which allows users to offload workflow tasks securely to the [Perlmutter](https://docs.nersc.gov/systems/perlmutter/) supercomputer.  Usage of this plugin is restricted to authorized users and is subject to NERSC's appropriate use [policies](https://www.nersc.gov/users/policies/appropriate-use-of-nersc-resources/). To request an account, refer to the documentation [here](https://docs.nersc.gov/accounts/).

## 1. Installation

To use this plugin with Covalent, install it using `pip`:

```shell
pip install covalent-nersc-plugin
```

## 2. Usage Example

This is an example of how a workflow can be adapted to use the NERSC executor plugin.

## 3. Configuration

There are many configuration options that can be passed into the class `ct.executor.NERSCExecutor` or by modifying the [Covalent config file](https://covalent.readthedocs.io/en/latest/how_to/config/customization.html) under the section `[executors.nersc]`.

Further detail about this plugin's configuration will be added to the core Covalent documentation in the near future. Check back soon!

## 4. Required NERSC Resources

In order to run Covalent workflows with this plugin, users will first need to have an account with NERSC.  Users will then need to create a [Superfacility API Client](https://docs.nersc.gov/services/sfapi/authentication/) in the Iris portal using the Red (highest) security level, with ingress allowed only from the Covalent server. Save the client ID and the private PEM key in a file on the Covalent server in the following format:

```shell
<client_id>
-----BEGIN RSA PRIVATE KEY-----
<private key contents>
-----END RSA PRIVATE KEY-----
```

This file will be ingested by the plugin when the Covalent server offloads workflow tasks to it.

## Getting Started with Covalent

For more information on how to get started with Covalent, check out the project [homepage](https://github.com/AgnostiqHQ/covalent) and the official [documentation](https://covalent.readthedocs.io/en/latest).

## Release Notes

Release notes for this plugin are available in the [Changelog](https://github.com/tylern4/covalent-nersc-plugin/blob/develop/CHANGELOG.md).

## Citation

Please use the following citation in any publications:

> W. J. Cunningham, S. K. Radha, F. Hasan, J. Kanem, S. W. Neagle, and S. Sanand.
> *Covalent.* Zenodo, 2022. https://doi.org/10.5281/zenodo.5903364

## License

Covalent is licensed under the GNU Affero GPL 3.0 License. Covalent may be distributed under other licenses upon request. See the [LICENSE](https://github.com/tylern4/covalent-nersc-plugin/blob/main/LICENSE) file or contact the [support team](mailto:support@agnostiq.ai) for more details.
