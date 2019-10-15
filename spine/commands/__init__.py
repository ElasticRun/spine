import logging
import click
from watchgod import run_process
from frappe.commands import pass_context, get_site

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def call_command(cmd, context):
    return click.Context(cmd, obj=context).forward(cmd)

@click.command('jsonworker')
@click.option('--queue', type=str, help="Queue name on which worker would listen.")
@click.option('--type', type=str, default="json", help="Data type that worker should expect as input.")
@pass_context
@click.option('--quiet', is_flag=True, default=False, help='Hide Log Outputs')
def start_json_worker(context, queue, type="json", quiet=False):
    from spine.spine_adapter.redis_client.redis_client import start_workers
    log.debug("jsonworker - queue - {}, type - {}, quiet - {}".format(queue, type, quiet))
    site = get_site(context)
    start_workers(site, queue, type, quiet)


@click.command('eventdispatcher')
@click.option('--queue', type=str, help="Queue name on which dispatcher should publish handler execution requests.")
@click.option('--type', type=str, default="json", help="Data type that worker should expect as input.")
@click.option('--quiet', is_flag=True, default=False, help='Hide Log Outputs')
@pass_context
def start_dispatcher_worker(context, queue, type="json", quiet=False):
    from spine.commands.dispatcher_command import start_dispatchers
    log.debug("dispatcher - queue - {}, type - {}, quiet - {}".format(queue, type, quiet))
    site = get_site(context)
    start_dispatchers(site, queue, type, quiet)

@click.command('aio-eventdispatcher')
@click.option('--queue', type=str, help="Queue name on which dispatcher should publish handler execution requests.")
@click.option('--noreload', is_flag=True, default=False)
@pass_context
def start_aio_dispatcher_worker(context, queue, noreload=False):
    site = get_site(context)
    log.debug("dispatcher - queue - {}, reloader - {}".format(queue, not noreload))

    if noreload:
        start_aio_dispatcher(site, queue)
    else:
        run_process(
            '../apps/',
            start_aio_dispatcher,
            args=(site, queue),
            min_sleep=4000,
        )

def start_aio_dispatcher(site, queue):
    from spine.commands.aio_dispatcher import start_dispatchers
    start_dispatchers(site, queue)

@click.command('er-new-site')
@click.argument('site')
@click.option('--db-name', help='Database name')
@click.option('--mariadb-root-username', default='root', help='Root username for MariaDB')
@click.option('--mariadb-root-password', help='Root password for MariaDB')
@click.option('--admin-password', help='Administrator password for new site', default=None)
@click.option('--verbose', is_flag=True, default=False, help='Verbose')
@click.option('--force', help='Force restore if site/database already exists', is_flag=True, default=False)
@click.option('--source_sql', help='Initiate database with a SQL file')
@click.option('--install-app', multiple=True, help='Install app after installation')
def new_site_patched(site, mariadb_root_username=None, mariadb_root_password=None, admin_password=None, verbose=False,
                     install_apps=None, source_sql=None, force=None, install_app=None, db_name=None):
    print("Patching new-site")
    from frappe.model.db_schema import DbManager as db_manager
    db_manager.create_user = create_user
    db_manager.grant_all_privileges = grant_all_privileges
    import frappe.commands.site
    frappe.init(site=site, new_site=True)
    frappe.commands.site._new_site(db_name, site, mariadb_root_username=mariadb_root_username, mariadb_root_password=mariadb_root_password,
              admin_password=admin_password,
              verbose=verbose, install_apps=install_app, source_sql=source_sql, force=force)
    if len(frappe.utils.get_sites()) == 1:
        frappe.commands.site.use(site)


def create_user(self, user, password, host=None):
    print("Using patched DbManager.create_user")
    # Create user if it doesn't exist.
    if not host:
        host = "%"
    if password:
        self.db.sql("CREATE USER '%s'@'%s' IDENTIFIED BY '%s';" % (user[:16], host, password))
    else:
        self.db.sql("CREATE USER '%s'@'%s';" % (user[:16], host))


def grant_all_privileges(self, target, user, host=None):
    print("Using patched DbManager.grant_all_privileges")
    if not host:
        host = "%"
    self.db.sql("GRANT ALL PRIVILEGES ON `%s`.* TO '%s'@'%s';" % (target,
                                                                  user, host))


commands = [
    start_json_worker,
    start_dispatcher_worker,
    start_aio_dispatcher_worker,
    new_site_patched,
]
