# For use with the just command runner, https://just.systems/

default:
  @just --list


export INSTALL_EL := '''
    (unless (package-installed-p 'pg)
       (package-vc-install "https://github.com/emarsden/pg-el" nil nil 'pg))
    (require 'pg)
'''
tmpdir := `mktemp -d`
init-el := tmpdir / "init.el"

# Check whether our package-vc-install instructions work on a pristine install.
installability:
   printf '%s' "$INSTALL_EL" > {{ init-el }}
   ls -l {{ init-el }}
   cat {{ init-el }}
   podman run --rm -ti -v {{ tmpdir }}:/tmp docker.io/silex/emacs:30.2-ci \
      ${EMACS:-emacs} -l /tmp/init.el



# Build the documentation.
#
# Uses mdbook and mdbook-admonish
# https://github.com/tommilligan/mdbook-admonish
doc:
   (cd doc && mdbook-admonish install .)
   (cd doc && mdbook build )
