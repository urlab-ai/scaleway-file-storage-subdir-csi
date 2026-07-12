Tu vas prendre en charge le developpement complet du chantier **Scaleway SFS Subdirectory CSI Driver**.

Le repository de travail est :

```text
/Users/samihamine/Developer/02_URLab/dev/scaleway-sfs-subdir-csi
```

Ce chantier est critique : tu developpes un pilote Kubernetes CSI qui gere des
montages RWX et des operations de cycle de vie sur des donnees. Une erreur peut
rendre des workloads indisponibles, monter le mauvais repertoire, detacher un
filesystem encore utilise ou supprimer les mauvaises donnees.

Le projet doit etre open source, production-grade et enterprise-grade des la
premiere version exploitable. Ce n'est pas un POC, un MVP ou un exemple de code.
Il doit rester simple, auditable, maintenable et reutilisable hors URLab.

Tu dois aller jusqu'au bout du chantier de maniere autonome, structuree et
rigoureuse, en respectant la specification officielle. L'objectif est
d'implementer toute la specification, de verifier section par section que rien
n'a ete oublie, puis de livrer un rapport final complet permettant une review
independante par des agents de controle.

---

## 1. Phase de comprehension obligatoire

Avant toute modification de code, place-toi dans le repository et lis les
sources suivantes dans cet ordre.

### 1. `AGENTS.md`

Lis integralement :

```text
AGENTS.md
```

Objectif :

- integrer les golden rules du projet ;
- comprendre que `docs/SPECIFICATION.md` est la source de verite ;
- respecter production-grade, enterprise-grade, KISS et no overengineering ;
- respecter la securite des donnees, la conformite CSI, la robustesse, la
  performance et la separation stricte des responsabilites ;
- comprendre la discipline Go, tests, documentation in-code et supply chain ;
- comprendre les regles de creation de ressources Scaleway et de controle des
  couts ;
- comprendre que tout changement de design ou de contrat doit mettre a jour la
  specification dans le meme changement.

### 2. `docs/SPECIFICATION.md`

Lis integralement, plusieurs fois si necessaire :

```text
docs/SPECIFICATION.md
```

C'est la source normative principale du chantier. Elle decrit notamment :

- le probleme produit et le modele subdirectory CSI ;
- les non-objectifs v1 ;
- l'identite publique du projet ;
- les contrats CSI Identity, Controller et Node ;
- le format du volume handle et du volume context ;
- les parents Scaleway File Storage et leur ownership exclusif ;
- les allocations Kubernetes et ownership records filesystem ;
- les etats durables et les machines d'etat create/delete/GC ;
- les regles de montage `virtiofs`, stage, publish, unpublish et unstage ;
- les limites physiques d'attachment et les preflights Node ;
- la capacite logique et le guardrail `statfs` ;
- les statuts Scaleway File Storage et Instance ;
- la coordination singleton par Lease ;
- les checkpoints, upgrades, recovery et decommission ;
- le binaire `csi-admin` ;
- le chart Helm, RBAC, security contexts et sidecars ;
- les tests unitaires, CSI sanity, Helm, Linux, kind et Scaleway E2E ;
- le contrat de release, les criteres d'acceptation et les limitations.

Ne traite pas cette specification comme une intention generale. Utilise-la
comme contrat d'implementation. Si un choix doit changer, modifie la spec dans
le meme changement et explique pourquoi.

### 3. Etat reel du repository

Avant de coder, execute et analyse :

```bash
git status --short
git log --oneline --decorate -10
find . -maxdepth 3 -type f -not -path './.git/*' | sort
```

Le repository peut etre encore minimal. Ne suppose pas qu'un composant existe.
Lis tous les fichiers presents avant d'ajouter une structure.

### 4. References provider et CSI

Consulte les references normatives pointees par la spec avant d'implementer les
parties correspondantes :

- specification Kubernetes CSI et conventions gRPC ;
- version pinnee de `kubernetes-csi/csi-test` ;
- driver officiel `scaleway/scaleway-filestorage-csi` au commit de reference
  fixe dans la spec ;
- version pinnee de `scaleway-sdk-go` ;
- API `file/v1alpha1` pour metadata et attachments ;
- API `instance/v1` pour Instances et attach/detach ;
- metadata service local Scaleway pour l'identite Node ;
- `csi-driver-nfs` et `nfs-subdir-external-provisioner` uniquement comme
  references du pattern subdirectory, sans dependre d'un endpoint NFS non
  documente.

Ne copie pas aveuglement le driver officiel. Reprends ses usages API fiables
quand ils sont compatibles avec la specification. Documente toute divergence
intentionnelle et preserve les licences/notices applicables au code copie.

---

## 2. Etat d'esprit attendu

Tu dois travailler comme un senior engineer / lead tech specialise Kubernetes,
CSI, Linux mounts et stockage distribue.

Regles non negociables :

- Production-grade des le premier patch.
- Data safety avant disponibilite ou confort.
- Robustesse maximale et erreurs explicites.
- KISS : Keep It Simple, Stupid.
- Pas d'overengineering.
- Pas de code spaghetti.
- Pas de refactor hors scope.
- Pas de CRD, base de donnees ou control plane additionnel sans revision
  explicite de la spec.
- Pas de controller HA invente en v1.
- Pas de fallback dangereux.
- Pas d'absence deduite d'un timeout ou d'une lecture indisponible.
- Pas de mutation filesystem sans ownership et mapping prouves.
- Pas de suppression ou unmount sur une cible ambigue.
- Separation stricte CSI, provider, mount, safety, state, coordination et
  recovery.
- Context cancellation et deadlines sur toute attente ou I/O.
- Polling borne avec backoff et jitter, jamais un sleep fixe comme contrat.
- Erreurs actionnables avec contexte, sans secret.
- Logs structures et labels de metrics bornes.
- Documentation in-code genereuse et exclusivement en anglais.
- Pas d'emojis dans le code, les commentaires, logs ou docs introduites.
- Pas de mega-file ni de package `utils`/`helpers` fourre-tout.
- Pas de nouvelle dependance sans besoin prouve et maintenance verifiee.
- Pas de ressource Scaleway reelle sans autorisation explicite de l'utilisateur.

---

## 3. Rappel du chantier

Le driver doit exposer beaucoup de PVC RWX logiques a partir d'un petit pool de
filesystems Scaleway File Storage existants.

Modele cible :

```text
PVC Kubernetes
  -> volume CSI logique
  -> sous-repertoire unique d'un parent Scaleway File Storage
  -> stage sur le noeud
  -> bind mount vers la cible du Pod
```

Le code applicatif et les workloads voient des PVC normaux. Ils ne connaissent
ni le parent physique ni le sous-repertoire. Le driver reduit ainsi le nombre de
File Storage attaches a chaque Instance tout en permettant des centaines ou des
milliers de volumes logiques.

Le projet comprend :

1. un binaire CSI Go avec services Identity, Controller et Node ;
2. un controller singleton qui gere ownership, allocations, capacity,
   reconciliation, create/delete et mounts de cycle de vie ;
3. un DaemonSet Node qui monte les parents puis stage/publish les sous-dossiers ;
4. des records Kubernetes et filesystem crash-durables ;
5. un binaire operateur `csi-admin` pour checkpoint, GC, upgrade et uninstall ;
6. un chart Helm public et securise ;
7. une suite complete de tests fake, Linux, kind et Scaleway ;
8. une release publique reproductible avec images, chart, binaires, checksums,
   SBOM et provenance.

---

## 4. Decisions actees a ne pas relitiguer

Tu ne dois pas rouvrir ces decisions. Tu dois les implementer correctement.

### Scope v1

- Les parents File Storage existent avant installation.
- Le driver ne cree, ne supprime et ne resize pas automatiquement les parents.
- Pas de snapshots, clones, quotas durs par PVC ou support cross-cloud.
- Pas de CRD ni de base de donnees.
- Les allocations utilisent des ConfigMaps deterministes.
- Le projet est autonome, public et sans dependance au code prive URLab.

### Identite et ownership

- `installationID` vient d'un Secret externe stable.
- `activeClusterUID` vient du namespace `kube-system`.
- Un parent appartient exclusivement a une installation.
- Claim racine fixe : `/.sfs-subdir-csi-owner.json`.
- Le claim est immutable, atomique, crash-durable et permanent en v1.
- Une copie du Secret dans un autre cluster n'autorise pas le parent.
- Le nom du Lease controller est fixe :
  `scaleway-sfs-subdir-csi-controller`.
- Le driver name, les handles et les schemas deviennent des contrats de
  compatibilite apres premiere utilisation.

### Volume handle et volume context

- Handle v1 : `sfs1:<logical-volume-id>:<mapping-hash>`.
- Longueur maximale : 128 bytes.
- `logicalVolumeID` est deterministe depuis driver name et
  `CreateVolumeRequest.name`.
- Le mapping hash couvre exactement les champs definis dans la spec.
- Le volume context est immutable, borne et valide champ par champ.
- `ControllerPublishVolume`, `NodeStageVolume` et `NodePublishVolume` exigent le
  context complet.
- `ValidateVolumeCapabilities` peut resoudre un context omis en lecture seule.
- `DeleteVolume` ne depend pas du volume context.

### Services CSI

- Les deux sockets implementent les trois RPC Identity.
- Plugin capability exacte : `CONTROLLER_SERVICE`.
- Controller capabilities exactes : create/delete et publish/unpublish.
- Node capability exacte : stage/unstage.
- Modes v1 : `SINGLE_NODE_WRITER` et `MULTI_NODE_MULTI_WRITER`.
- `SINGLE_NODE_WRITER` autorise seulement le retry identique sur la meme cible.
- `MULTI_NODE_MULTI_WRITER` porte le cas RWX principal.
- Pas de topology, expansion logique, snapshot, clone ou volume stats en v1.

### Create et durable state

- Un record d'allocation ConfigMap est cree avant success `CreateVolume`.
- Etats fermes : `Reserved`, `CreatingDirectory`, `Ready`, `Deleting`,
  `Archived`, `Deleted`, `Retained`.
- Pas d'etat generique `Failed`.
- Le record d'ownership filesystem est une preuve distincte.
- Les dual writes suivent exactement l'ordre et la table de crash recovery de
  la spec.
- Les erreurs laissent le dernier etat durable reprenable.
- Le nom de requete create n'est jamais reutilise dans une installation.

### Delete et GC

- Policies : `archive` par defaut, `delete` opt-in, `retain`.
- Aucune operation destructive sans mapping, ownership, path et fences valides.
- `DeleteVolume` vide retourne `InvalidArgument`.
- Un ID non vide manifestement etranger retourne success sans lookup ni
  tombstone.
- Un handle driver parseable avec mapping incoherent fail closed.
- L'absence valide peut creer uniquement un tombstone `deletedUnknown` minimal.
- Une lecture indisponible n'est jamais une absence.
- `deleteRemoveStartedAt` doit etre durable des deux cotes avant removal.
- Archive, retain et GC conservent la capacite tant que les donnees existent.
- Les tombstones permanents ne sont jamais supprimes en v1.
- GC est manuel, audite, dry-run capable et execute par le leader.

### Filesystem safety et mounts

- Tous les chemins passent par un package safety dedie.
- Resolution no-follow, pas de traversal, pas de symlink escape.
- Pas de suppression a travers un mount boundary.
- Suppression en deux etapes via quarantine `.deleted`.
- Les metadata et transitions filesystem utilisent les barriers de durabilite
  definies par la spec.
- Les quatre RPC Node prennent un lock context-aware par volume.
- Le lock parent est imbrique dans le lock volume, jamais l'inverse.
- Stage/publish prouvent la source exacte du mount.
- Unpublish/unstage refusent un mount foreign, alias, remplace ou stacked.
- Le driver ne cree ou supprime jamais le staging directory appartenant au CO.
- Les parent mounts peuvent rester chauds pendant le fonctionnement normal.

### Provider Scaleway

- Region, project, filesystem, Instance et commercial type sont valides.
- Les listes d'attachments sont paginees et reconciliees avec
  `Server.Filesystems`.
- L'attach limit vient du live `MaxFileSystems` et d'une allowlist testee.
- Les states Instance et File Storage suivent les matrices fermees de la spec.
- Un state inconnu ou illisible fail closed.
- Attach poll jusqu'a `available`, deadline par defaut 10 minutes.
- Detach uniquement pour les chemins explicitement autorises par la spec.
- Le node utilise le metadata service local et ne recoit pas de credentials.
- File Storage shrink est non supporte ; une baisse observee declenche
  `critical-size-regression`.

### Capacity et pool

- Selection v1 : `least-allocated` avec tie-break deterministe.
- Un parent `draining` ne recoit plus de nouveaux volumes mais reste operable.
- Capacite logique et reserve sont calculees avec arithmetique checked.
- `statfs` utilise `f_bavail * f_bsize` avec multiplication checked.
- Le seuil physique est le max de `minFreeBytes` et du pourcentage arrondi au
  plafond.
- Pas de hard quota par sous-repertoire ; ce risque est documente et monitore.

### Controller, Lease et recovery

- Exactement un controller replica en v1.
- Deployment `Recreate`.
- Lease obligatoire mais ne constitue pas un storage fence.
- Au moins deux noeuds Ready compatibles doivent pouvoir accueillir le
  controller en production.
- Pas d'auto takeover apres crash non fence.
- Handoff normal seulement via graceful-release marker exact.
- Abnormal takeover et missing-Lease recovery exigent fencing provider et
  approval Secret immutable selon la spec.
- Checkpoint seulement sous quiesce complet.
- Recovery cross-cluster hors scope.
- Decommission parent est une procedure offline explicite.
- Un parent decommissionne n'est pas remonte uniquement pour relire ses
  tombstones historiques.

### Helm, securite et operations

- Namespace dedie avec Pod Security privileged explicite.
- Controller et Node ServiceAccounts distincts.
- Credentials uniquement dans le controller.
- HostPaths minimaux et disjoints.
- Images driver/sidecars par digest immutable en release.
- Sidecars standards et versions testees.
- `priorityClassName: system-cluster-critical` par defaut pour le controller.
- Direct `helm uninstall` non supporte avant `csi-admin uninstall prepare`.
- `csi-admin` est un artifact public versionne, checksum-verifiable et soumis a
  handshake de protocole.

---

## 5. Plan de developpement obligatoire

Apres lecture, produis un plan actionnable avant toute modification. Il doit
couvrir, dans un ordre qui garde le repository compilable :

1. Identite publique finale et Go module provisoire/final selon informations
   disponibles.
2. Scaffold minimal : Go module, Makefile, Dockerfile, chart, CI.
3. Types purs : IDs, handles, hashes, capacities et schemas durables.
4. Validation stricte des configurations et Helm values.
5. Allocation records ConfigMap et optimistic concurrency.
6. Parent claim et ownership records filesystem atomiques.
7. Fake clock, fake provider, fake Kubernetes et fake mounter.
8. Service Identity sur les deux sockets.
9. Provider Scaleway API et error mapping.
10. Metadata Node Scaleway et `NodeGetInfo`.
11. Parent attachment inventory, limits et attach state machine.
12. Pool accounting, metadata refresh et `statfs` guardrail.
13. CreateVolume state machine et idempotency.
14. ValidateVolumeCapabilities.
15. Controller publish/unpublish et published-node fences.
16. Node stage/publish/unpublish/unstage avec mount graph validation.
17. Delete archive/delete/retain state machine.
18. GC admin flow et tombstone compaction.
19. Startup reconciliation et crash repair ferme.
20. Lease singleton, graceful shutdown et abnormal takeover.
21. Checkpoint, restore same-cluster et upgrade preflight.
22. Parent lifecycle active/draining et offline decommission.
23. Safe uninstall complet.
24. `csi-admin` version/protocol/artifact contract.
25. Metrics, events, logs et sample alerts.
26. Helm chart complet, RBAC, security contexts, probes et sidecars.
27. Unit tests et race tests.
28. CSI sanity split controller/node.
29. Helm and kind integration tests.
30. Privileged Linux mount/filesystem tests.
31. E2E Scaleway scripts cost-safe et cleanup-safe.
32. Documentation publique et operations guide.
33. Release automation, checksums, SBOM et provenance.
34. Auto-review spec section par section.

N'essaie pas de tout mettre dans quelques fichiers. Le plan doit identifier les
packages responsables et les interfaces minimales entre eux.

---

## 6. Developpement attendu

### 6.1 Structure Go

Respecte la structure recommandee par la spec, adaptee seulement si le code reel
demontre une organisation plus simple :

```text
cmd/scaleway-sfs-subdir-csi/
cmd/csi-admin/
pkg/driver/
pkg/scaleway/
pkg/pool/
pkg/volume/
pkg/safety/
pkg/mount/
pkg/k8s/
pkg/coordination/
pkg/recovery/
charts/scaleway-sfs-subdir-csi/
deploy/examples/
docs/
hack/e2e/
```

Chaque package a une responsabilite et une documentation claire. Les interfaces
provider/mounter servent les tests et une vraie boundary, pas une abstraction
generique speculative.

### 6.2 Domain et schemas durables

- Implementer handles, mapping hash, request hash et context canonique.
- Valider toutes les limites bytes avant mutation.
- Implementer exactement les schemas `detailed`, `compactDeleted` et
  `deletedUnknown`.
- Rejeter champs inconnus, schema inconnu et combinaison state/capacity invalide.
- Checksums, revisions et atomic replacement crash-durable.
- Aucun reader ne doit inventer un champ absent depuis la config courante.
- Ajouter fixtures de compatibilite des la premiere release.

### 6.3 Kubernetes state

- ConfigMap par volume avec nom deterministe.
- Creation atomique comme lock d'idempotency.
- ResourceVersion/CAS pour chaque update.
- Index/list borne, pas d'API call par tombstone si list/informer suffit.
- PV, VolumeAttachment, Node, CSINode et namespace lus avec RBAC minimal.
- Aucun delete permission sur les tombstones permanents.

### 6.4 Provider Scaleway

- Client fortement type autour du SDK pinne.
- Pagination complete.
- Validation project/region/zone/resource type.
- Union dedupee des attachments.
- Matrices de states fermees.
- Polling context-aware.
- Re-read apres resultat ambigu.
- IAM documente, sans demander `FileStorageFullAccess` inutilement.
- Node sans API credential.

### 6.5 Controller service

- `CreateVolume` idempotent, transactionnel au niveau des records, crash-safe.
- Selection parent seulement pour une nouvelle allocation.
- Replay utilise le parent persiste.
- `DeleteVolume` respecte unknown-ID, absence et corruption.
- `ControllerPublishVolume` attache puis persiste le fence avant success.
- `ControllerUnpublishVolume` ne nettoie un fence qu'avec preuve suffisante.
- Locks et global mutation semaphore dans l'ordre fixe.
- Erreurs gRPC exactes par RPC.

### 6.6 Node service

- Metadata identity locale.
- Mount parent `virtiofs` exact.
- Bind stage puis bind publish.
- Lecture live de la mount table.
- No-follow path validation.
- Idempotency stricte.
- Read-only bind correct.
- Cleanup uniquement des directories crees/possedes par le driver.
- Pas de provider API authentifie.

### 6.7 Filesystem lifecycle

- Parent claim atomique no-overwrite.
- Ownership metadata temp file, fsync, rename, directory fsync, read-back.
- Directory create/chown/chmod avec durability barriers.
- Archive/quarantine rename avec sync des deux parents.
- Recursive delete descriptor-relative sans symlink/mount escape.
- Tombstone terminal seulement apres absence durable.
- Crash injection a chaque frontiere.

### 6.8 Coordination et recovery

- Lease fixe non rendu par Helm.
- Graceful marker borne et CAS.
- Watchdog de leadership.
- Shutdown deadline distincte des deadlines normales.
- Approval Secrets get-only et immutables.
- Checkpoint O(parents + images), objets detailles dans package externe.
- Missing-Lease recovery offline, same-cluster uniquement.
- Node config generation pour rollout N/N-1.

### 6.9 Helm et packaging

- `values.yaml` et `values.schema.json` complets.
- Cross-field validation critique.
- Controller Deployment singleton `Recreate`.
- Node DaemonSet sur tous les noeuds Linux eligibles.
- Sockets, hostPaths et mount propagation exacts.
- Resource requests/limits pour chaque container.
- Startup/readiness/liveness separes.
- Digests obligatoires en release.
- RBAC derive des sidecars pinnes et permissions driver minimales.

### 6.10 `csi-admin`

- Commandes checkpoint prepare/resume.
- GC dry-run/execute.
- Upgrade preflight.
- Uninstall prepare.
- Version handshake avant toute mutation.
- Operations idempotentes par request ID.
- Audit structure et actionnable.
- Aucun edit direct sauvage de lifecycle state.

### 6.11 Documentation publique

- README installable par un utilisateur externe.
- Architecture et limitations reelles.
- Operations et troubleshooting.
- IAM et credentials.
- Recovery, checkpoint, upgrade, decommission et uninstall.
- Disclaimer community project / not official Scaleway product.
- Attribution URLab et MIT.
- Aucun chemin local, domaine prive, kubeconfig ou secret URLab.

---

## 7. Discipline de modification

Pendant le developpement :

- lis avant de modifier ;
- garde chaque fichier focalise ;
- documente packages, exported symbols, schemas et invariants en anglais ;
- privilegie les fonctions pures pour validation/state/capacity ;
- propage les contexts ;
- ne masque jamais une erreur ;
- ne fais jamais confiance a un path, mount table, provider response ou objet
  Kubernetes non valide ;
- n'utilise pas `context.Background()` pour contourner une cancellation de RPC ;
- ne lance pas de goroutine sans ownership, cancellation et attente de fin ;
- n'utilise pas `panic` ou `log.Fatal` dans une librairie ;
- n'ajoute pas de sleeps pour stabiliser les tests ;
- n'ajoute pas d'override production qui bypass ownership, fencing ou preflight ;
- n'introduis pas de compatibilite legacy non specifiee ;
- ne cree pas de montage stacked pour reparer un retry ;
- ne detache jamais sur un chemin non autorise ;
- ne modifie pas la spec apres coup pour justifier un code incorrect ;
- garde spec, code, tests et operations synchronises dans le meme changement.

---

## 8. Tests et verification

Tu dois ajouter les tests requis par la spec. Les tests ne sont pas une phase
optionnelle apres l'implementation.

### 8.1 Unit tests

Couvre au minimum :

- handle/context/hash/canonicalisation et limites bytes ;
- schemas durables et states fermes ;
- create replay, no-op, conflicts et capacity range ;
- delete foreign/absent/corrupt/unavailable ;
- dual-write crash points create/publish/unpublish/delete/GC ;
- path safety, symlinks, mount boundaries et reserved paths ;
- pool selection, reserves, overflow et `statfs` exact ;
- matrices File Storage/Instance/attachment ;
- attach pagination, union, ambiguity, timeout et cancellation ;
- access modes et status gRPC exacts ;
- locks, semaphore, shutdown et Lease ;
- checkpoint, approval, compaction et decommission ;
- admin protocol/version mismatch ;
- scale envelope fake.

### 8.2 CSI sanity

- Pinner `kubernetes-csi/csi-test`.
- Executer la suite sur socket controller et socket node.
- Prouver que les tests Controller ont reellement tourne.
- Ajouter tests Identity directs sur les deux sockets.
- Ne pas presenter une suite partiellement skippee comme conforme.

### 8.3 Linux privileged tests

Les fakes ne suffisent pas pour :

- mount source et mount graph ;
- stacked mounts ;
- stage/publish/unpublish/unstage concurrents ;
- symlink swap ;
- nested mount deletion ;
- exact unmount ;
- read-only bind ;
- fsync directory et durability barriers ;
- crash/restart autour des transitions filesystem.

Ces tests doivent tourner dans un environnement Linux isole et ne jamais viser
le host de developpement sans isolation explicite.

### 8.4 Helm et kind

- `helm lint` ;
- schema/negative rendering ;
- RBAC ;
- Secrets non rendus ;
- security contexts et mount propagation ;
- digests ;
- probes ;
- sidecar args/timeouts/workers ;
- Lease fixe sans objet mutable rendu ;
- fake provider chart install dans kind ;
- restarts, rollouts, checkpoint et uninstall fake.

### 8.5 Real Scaleway E2E et couts

Ne cree aucune ressource reelle sans demander une autorisation explicite juste
avant l'operation. Des credentials disponibles ne constituent pas une
autorisation.

Avant chaque session reelle :

1. verifier qu'il s'agit d'un Project de test dedie ;
2. annoncer les ressources creees ;
3. choisir le cluster/noeuds/filesystems minimaux ;
4. estimer et afficher le cout horaire ;
5. generer un run ID unique et des tags ownership ;
6. afficher le plan de cleanup et les commandes ;
7. attendre le `GO` explicite de l'utilisateur.

Les scripts doivent :

- exiger project ID, region, run ID, prefix et confirmation deliberée ;
- supporter dry-run ;
- utiliser uniquement des IDs exacts pour cleanup ;
- refuser toute suppression large ou ambigue ;
- ne jamais supprimer une ressource preexistante ou reutilisee ;
- nettoyer apres succes et echec controle ;
- imprimer l'inventaire final des ressources facturables ;
- imprimer les IDs survivants et la commande exacte si cleanup incomplet.

Ne laisse pas un cluster allume entre sessions. Les tests hard failure, stop,
delete Instance et detach utilisent exclusivement un cluster et des parents
jetables et tags.

### 8.6 Commandes locales minimales

Selon l'etat du repository, execute au minimum :

```bash
gofmt -l .
go test ./...
go test -race ./...
go vet ./...
golangci-lint run
helm lint charts/scaleway-sfs-subdir-csi
```

Execute aussi CSI sanity, chart tests, kind integration et Linux privileged
tests via les runners documentes du repository.

Si une commande ne peut pas etre executee, documente :

- commande exacte ;
- erreur exacte ;
- cause ;
- impact ;
- risque residuel ;
- verification alternative ;
- condition necessaire avant release.

---

## 9. Phase de controle obligatoire avant livraison

Quand tu penses avoir termine :

1. Relis integralement `AGENTS.md`.
2. Relis integralement `docs/SPECIFICATION.md`.
3. Execute `git status --short`.
4. Execute `git diff --stat`.
5. Execute `git diff`.
6. Lis tous les fichiers non trackes.
7. Verifie chaque section normative de la spec.
8. Verifie qu'aucun choix de code n'a diverge sans update de spec.
9. Verifie les trois services CSI et les capabilities exactes.
10. Verifie le volume handle/context et leurs limites.
11. Verifie la source de verite allocation et la preuve ownership.
12. Verifie tous les etats et dual-write crash windows.
13. Verifie les barriers de durabilite filesystem.
14. Verifie les protections symlink/path/mount boundary.
15. Verifie les locks et leur ordre.
16. Verifie chaque chemin de detach.
17. Verifie les matrices provider fermees.
18. Verifie pagination et reconciliation des inventories.
19. Verifie capacity, overflow et `statfs`.
20. Verifie singleton, Lease, graceful et takeover.
21. Verifie checkpoint, restore, decommission et upgrade.
22. Verifie safe uninstall dans l'ordre exact.
23. Verifie le chart, RBAC, Secrets et privileges.
24. Verifie que le Node ne recoit aucun credential.
25. Verifie les metrics et labels bornes.
26. Verifie que `csi-admin` distribue est utilise dans les release tests.
27. Verifie que CSI sanity Controller n'est pas skippe.
28. Verifie les tests Linux reels.
29. Verifie qu'aucune ressource Scaleway de test ne reste active.
30. Verifie qu'aucun secret ou identifiant sensible n'est tracke.
31. Relance tous les tests pertinents apres les dernieres corrections.

Scans utiles :

```bash
rg -n "TODO|FIXME|HACK|panic\(|log\.Fatal" --glob '*.go' .
rg -n "context\.Background\(|time\.Sleep\(" --glob '*.go' .
rg -n "latest|imagePullPolicy|privileged|hostPath|mountPropagation" charts deploy
rg -n "DetachServerFileSystem|AttachServerFileSystem|ListAttachments|Server\.Filesystems" .
rg -n "RemoveAll|Remove|Rename|Unmount|Mount|EvalSymlinks|filepath\.Join" pkg
rg -n "Deleted|Deleting|Archived|Retained|deleteRemoveStartedAt|gcRemoveStartedAt" .
rg -n "SCW_SECRET_KEY|SCW_ACCESS_KEY|kubeconfig|token|password" .
```

Qualifie chaque resultat ; ne corrige pas mecaniquement des faux positifs.

---

## 10. Rapport final attendu

Ton rapport final doit contenir :

1. **Resume executif**
   - ce qui a ete developpe ;
   - etat global ;
   - elements non termines.

2. **Mapping spec -> implementation**
   - section normative ;
   - statut implemented/partial/not implemented ;
   - fichiers et tests correspondants.

3. **Architecture et packages**
   - responsabilite de chaque package ;
   - dependances entre packages ;
   - justification KISS.

4. **Fichiers crees/modifies**
   - liste complete ;
   - role de chaque fichier.

5. **CSI contracts**
   - Identity ;
   - Controller ;
   - Node ;
   - capabilities ;
   - access modes ;
   - error mapping.

6. **Durable state et recovery**
   - allocation records ;
   - ownership records ;
   - state machines ;
   - crash recovery ;
   - tombstones ;
   - checkpoints.

7. **Provider Scaleway**
   - APIs ;
   - states ;
   - attachment inventory ;
   - IAM ;
   - metadata service ;
   - compatibilite testee.

8. **Mount et filesystem safety**
   - stage/publish ;
   - unpublish/unstage ;
   - paths ;
   - symlinks ;
   - mount boundaries ;
   - fsync/durability.

9. **Capacity et pools**
   - accounting ;
   - statfs ;
   - selection ;
   - active/draining ;
   - resize observation.

10. **Coordination et operations**
    - singleton ;
    - Lease ;
    - graceful/abnormal takeover ;
    - upgrade ;
    - decommission ;
    - uninstall ;
    - `csi-admin`.

11. **Helm, securite et supply chain**
    - RBAC ;
    - ServiceAccounts ;
    - privileges ;
    - Secrets ;
    - images/digests ;
    - release artifacts.

12. **Tests ajoutes/modifies**
    - unit ;
    - race ;
    - sanity ;
    - Helm ;
    - kind ;
    - Linux ;
    - Scaleway E2E.

13. **Ressources Scaleway et couts**
    - autorisations obtenues ;
    - ressources creees ;
    - cout estime/reel ;
    - cleanup ;
    - inventaire final.

14. **Commandes executees**
    - commandes exactes ;
    - resultats ;
    - erreurs ;
    - risques residuels.

15. **Documentation**
    - README ;
    - operations ;
    - troubleshooting ;
    - in-code docs ;
    - specification mise a jour.

16. **Auto-review finale**
    - git status/diff ;
    - conformite spec ;
    - securite ;
    - KISS/no overengineering ;
    - risques residuels ;
    - points a controler independamment.

17. **Conclusion**
    - indique clairement si le chantier est pret pour review.

---

## 11. Informations manquantes a completer

La specification laisse volontairement certaines informations a fixer avant la
premiere release publique :

- nom CSI final sous un domaine controle ou approuve ;
- organisation GitHub et URL publique du repository ;
- registry publique des images ;
- distribution publique du chart Helm ;
- plateformes binaires `csi-admin` officiellement supportees ;
- versions finales Go, Kubernetes/Kapsule, SDK, CSI modules et sidecars ;
- Project Scaleway dedie aux E2E, budget et politique de conservation des
  preuves.

Ne laisse pas de placeholder silencieux dans une release. Avance sur tout ce qui
n'en depend pas et remonte clairement chaque decision encore necessaire.

---

## 12. Objectif final

L'objectif n'est pas une implementation partielle.

L'objectif est de livrer **Scaleway SFS Subdirectory CSI Driver** complet,
coherent, robuste, documente et production-ready, conforme a :

```text
AGENTS.md
docs/SPECIFICATION.md
```

Avance jusqu'au bout, verifie ton travail, corrige les problemes identifies,
execute les validations pertinentes et livre un rapport permettant une review
independante. Ne considere pas le chantier termine tant que chaque exigence
importante de la specification n'a pas une implementation et une preuve de test
ou une limitation explicitement qualifiee.
