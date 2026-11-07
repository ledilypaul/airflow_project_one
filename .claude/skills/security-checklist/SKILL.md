---
name: security-checklist
description: Checklist de sécurité IT pour une plateforme Airflow + Spark + Postgres + Docker - secrets, authentification, dépendances, réseau, conteneurs, code. À charger pour tout audit de sécurité, revue de config, changement de dépendances ou avant une mise en ligne.
when_to_use: "Mots-clés sécurité, secret, mot de passe, CVE, vulnérabilité, docker-compose, Kubernetes, exposition, permissions, injection."
---

# Checklist sécurité — airflow_project_one

Pour chaque point : ✅ conforme / ❌ non conforme / ➖ non applicable, avec la preuve (fichier:ligne ou sortie de commande). Ne jamais recopier la valeur d'un secret.

## A. Secrets et dépôt Git (dépôt **public**)
- [ ] Aucun secret suivi : `git ls-files | grep -iE '\.env|password|secret|\.pem|\.key|airflow\.cfg|\.db$|\.generated$'`
- [ ] Aucun secret dans l'historique : `gitleaks detect --no-banner` (ou `git log -p -S '<motif>'` ciblé)
- [ ] `.gitignore` couvre `.env*`, `airflow.cfg`, `airflow.db`, `logs/`, `*.generated`, `data/`, sorties de notebooks
- [ ] Notebooks commités sans sorties (`nbstripout`)
- [ ] Scripts shell sans identifiants en dur

## B. Airflow
- [ ] Auth manager configuré volontairement (Simple Auth Manager = dev uniquement)
- [ ] `fernet_key` définie hors dépôt (chiffrement des Connections/Variables)
- [ ] Clés secrètes de l'API (`secret_key`, JWT) définies hors dépôt et identiques entre composants
- [ ] Identifiants métier en **Connections** ou secrets backend, pas dans le code ni `.env` lu par le code
- [ ] `expose_config = False`, exemples désactivés
- [ ] Version Airflow et providers sans CVE connue (comparer aux notes de version de sécurité)

## C. Dépendances
- [ ] Une seule source de vérité (`pyproject.toml`), `uv.lock` commité et à jour (`uv lock --check`)
- [ ] Versions Airflow/providers comparées au fichier de constraints officiel de la version (écarts justifiés)
- [ ] `uvx pip-audit -r <(uv export --no-hashes)` sans vulnérabilité haute/critique non traitée
- [ ] Pas de dépendances inutiles (surface d'attaque) — ex. bibliothèques LLM sans usage dans le code
- [ ] Dependabot ou Renovate activé

## D. Réseau et conteneurs
- [ ] Pas d'identifiants par défaut (`airflow/airflow`, `postgres/postgres`) hors dev local
- [ ] Ports publiés limités au strict nécessaire et liés à `127.0.0.1` en local
- [ ] Redis protégé par mot de passe et non exposé
- [ ] Images épinglées par version (idéalement digest), utilisateur non root
- [ ] `trivy fs .` / `trivy image` sans critique non traitée
- [ ] En Kubernetes : secrets en `Secret`/External Secrets, NetworkPolicies, `securityContext` restrictif

## E. Code
- [ ] SQL toujours paramétré (`text()` + paramètres liés), jamais de f-string avec une entrée
- [ ] `yaml.safe_load` uniquement (jamais `yaml.load` sans Loader sûr)
- [ ] Pas de `subprocess`/`BashOperator` construit à partir d'entrées non maîtrisées (noms de fichiers !)
- [ ] Chemins de fichiers validés (pas de traversée hors des répertoires sources configurés)
- [ ] Logs sans données sensibles (identifiants, URL de connexion complètes)
- [ ] `bandit -r pipeline utils storage dags -q` sans sévérité haute

## F. CI/CD
- [ ] Secrets CI dans GitHub Secrets, jamais dans le workflow
- [ ] Revue sécurité automatique des PR (action `anthropics/claude-code-security-review` ou équivalent) — attention, elle n'est pas durcie contre l'injection de prompt : la réserver aux PR de confiance
- [ ] Branch protection sur `main`
