# soft-drain

## 왜 만드는가

노드를 빼려면 `kubectl drain`을 쓰는데, replicas가 1인 워크로드는 Pod이 먼저 죽고 새로 뜨는 동안 장애가 난다. 2벌을 띄워 HA를 만들자니 비용이 두 배고, PDB를 걸면 drain이 429로 튕길 뿐 Pod이 옮겨지지는 않는다.

soft-drain은 **새 Pod을 먼저 띄우고 Ready가 된 다음 옛 Pod을 없앤다.** 용량이 비는 순간이 없다.

## 원리

ReplicaSet에는 두 가지 성질이 있다.

1. selector에 맞고 주인 없는 Pod을 보면 자기 자식으로 데려간다.
2. 자식이 replicas보다 많으면 하나를 지우는데, `pod-deletion-cost`가 낮은 쪽을 먼저 지운다.

soft-drain은 이 둘을 이어 붙인다. 옮길 Pod과 똑같은 Pod을 하나 더 만들되 `pod-template-hash` 라벨은 빼둔다. 그러면 selector에 안 걸려서 ReplicaSet이 데려가지 않는다. 그 Pod이 Ready가 되면 hash를 붙인다. ReplicaSet이 데려가면서 자식이 하나 늘고, 늘어난 만큼 하나를 지운다. `pod-deletion-cost`가 음수인 옛 Pod이 지워진다.

**Pod을 옮기는 일은 ReplicaSet이 한다. soft-drain은 재료만 놓아준다.**

`pod-template-hash`를 처음부터 붙이면 안 된다. ReplicaSet이 만들자마자 데려가는데 그때 새 Pod은 아직 Pending이고, 삭제 순서에서 Pending과 NotReady가 `pod-deletion-cost`보다 앞이라 방금 만든 Pod이 먼저 죽는다.

`pod-template-hash`는 ReplicaSet selector에는 들어가고 Service selector에는 안 들어간다. 그래서 대체 Pod은 Ready가 되는 즉시 Endpoints에 올라가고 입양 전후로 빠지지 않는다. 엔드포인트 갱신이 Pod당 한 번뿐이다.

**보장하는 것은 노출이지 어느 Pod이 지워지는가가 아니다.** `pod-deletion-cost`는 삭제 정렬의 4순위 힌트다. 넘기는 순간 무관한 Pod이 NotReady면 ReplicaSet은 그쪽을 지우고 우리 타깃은 남는다. 그래도 Ready인 Pod을 먼저 늘린 다음 초과분이 지워지므로 노출은 `N` 밑으로 내려가지 않고, 남은 타깃은 다음 라운드에 다시 시도된다.

## 쓰는 법

```
kubectl label node node-01 soft-drain.com/drain=true      # 시작
kubectl get nodes -l soft-drain.com/state=Complete        # 완료 확인
kubectl label node node-01 soft-drain.com/drain-          # 취소
kubectl uncordon node-01                                 # 이것도 취소다 (state=Cancelled 로 남는다)
```

끝난 노드는 cordon된 채로 남는다. 그 다음에 drain을 하든 노드를 리부팅하든 soft-drain이 상관할 일이 아니다.

### kubectl 플러그인

`kubectl soft-drain`은 위 네 줄의 포장이다. **쓰는 것은 drain 라벨 하나뿐이고 나머지는 읽기다** — 서버 쪽 표면은 늘지 않는다. 문법은 `git stash`형이다 — 맨몸+노드가 주 동작이고, 나머지는 서브커맨드다. `status`와 `release`는 예약어다.

```
kubectl soft-drain node-01                 # 라벨을 붙이고 Complete까지 진행을 보여준다 (--wait=false, --timeout)
kubectl soft-drain status                  # 현황판: 관여 중인 노드·남은 타깃·대체 Pod
kubectl soft-drain status node-01 -o json  # 특정 노드, 기계용 (json|yaml)
kubectl soft-drain release node-01         # 라벨을 걷고 복원을 기다린다
```

release는 진행 중이면 취소가 되고 Complete면 관리 종료가 된다 — 실체는 같은 라벨 제거다. `kubectl uncordon`도 취소지만 라벨과 Cancelled 래치가 남는 점이 다르다 — release는 전부 걷는다. `--timeout`이 터지면 Pending 대체 Pod과 스케줄러 메시지를 보여주고 0이 아닌 코드로 나간다 — "막혔을 때 보는 법"의 자동화다. 중간에 끊어도 라벨은 남으므로 drain은 계속된다. `state=Cancelled`인 노드에 다시 drain을 걸면 라벨을 걷어 복원시킨 뒤 다시 붙인다.

현황판에 "언제부터"는 없다 — 컨트롤러가 무기억이라 시작 시각을 어디에도 기록하지 않는다. `-o`의 몫은 플러그인만 계산할 수 있는 집계(타깃·대체 Pod 상태·스케줄러 메시지)다. 노드명 목록이 필요한 기계는 라벨 조회가 정석이다: `kubectl get nodes -l soft-drain.com/state=Complete -o name`.

`kubectl drain`의 `--ignore-daemonsets`, `--delete-emptydir-data`, `--force`는 없다. eviction 전제의 개념이라 여기 해당이 없다.

## 컨트롤러가 하는 일

```
1. drain 라벨이 붙은 노드를 cordon한다
2. 그 노드의 Deployment Pod(타깃)에 pod-deletion-cost 를 음수로 박는다
3. 타깃마다 대체 Pod이 하나씩 있도록 맞춘다
4. 대체 Pod이 Ready가 되면 pod-template-hash 를 붙여 ReplicaSet이 데려가게 한다
5. 타깃이 없어질 때까지 반복한다
6. 타깃이 없으면 완료 표시를 한다
```

**어떤 상태도 기억하지 않는다.** 매번 클러스터를 다시 보고 전부 다시 판정하므로 중간에 죽어도 다음 라운드가 이어서 한다.

**읽기는 API 서버에서 직접 한다.** watch는 다시 볼 때를 알려주는 데만 쓴다. 캐시가 뒤처진 상태를 설계가 감당하기 시작하면 조건이 급격히 복잡해진다. 부하가 문제가 되면 그때 캐시를 붙인다.

### 1. 노드 마킹

`drain` 라벨이 있으면 cordon한다. 우리가 실제로 값을 바꿨을 때만 `cordoned-by-controller` 어노테이션을 단다. 사람이 미리 걸어둔 cordon을 나중에 우리가 푸는 일을 막기 위해서다.

**cordon은 준비 작업이 아니라 이 반복이 끝나는 이유다.** cordon을 걸어두면 그 노드에는 Pod이 새로 안 뜬다. 빼야 할 Pod이 늘어날 일이 없으니 하나씩 빼다 보면 언젠가 바닥이 난다. 예외는 `unschedulable`을 tolerate하는 워크로드뿐인데, 그건 빼도 그 자리에 다시 앉아서 줄지를 않는다. 그래서 거기서 멈춘다(4번).

`drain` 라벨이 사라지면 되돌린다 — 우리 값이 박힌 `pod-deletion-cost`를 걷고, `cordoned-by-controller`가 있으면 uncordon하고, `state` 라벨을 지운다.

**누가 uncordon하면 관여를 접는다 — 진행 중이든 끝난 뒤든.** `state`가 `InProgress`나 `Complete`인데 노드가 `unschedulable`이 아니면 그렇게 된 것이다 — 두 상태 모두 cordon을 확인한 뒤에만 붙기 때문에 이 조합이 곧 증거다. 진행 중이라면 cordon은 종료를 보장하던 전제라서 전제가 사라진 채 계속할 수 없고, 끝난 뒤라면 cordon 소유권을 넘겨받은 사람이 노드를 다시 쓰기로 결정한 것이다. 어느 쪽이든 다시 cordon해서 사람과 싸우지 않는다. cost를 걷고 `state=Cancelled`를 붙인 뒤 손을 뗀다. 대체 Pod은 회수 경로가 걷는다. `Cancelled`는 래치다 — 라벨을 걷으면 지워지고, 다시 하려면 라벨을 걷었다가 다시 붙인다.

`Complete`를 접지 않고 두면 그 노드가 삭제 자석이 된다. 방금 비워져 가장 한가한 노드가 열렸으니 스케줄러는 다른 drain의 대체 Pod을 정확히 거기 앉히고, 착지 검사는 앉는 족족 지운다. 클러스터가 작을수록 모든 drain이 그 노드로 빨려 들어간다.

### 2. 타깃 표시

타깃은 **그 노드 위에서 owner가 ReplicaSet이고 그 ReplicaSet의 owner가 Deployment인 Pod**이다. phase가 `Failed`나 `Succeeded`인 Pod은 빼고 센다 — ReplicaSet도 active로 세지 않는 Pod이라 대체는 이미 딴 곳에 만들어져 있고, 노드에 남은 시체가 완료 판정만 막는다.

`controller.kubernetes.io/pod-deletion-cost = -2147483648`을 쓴다. 타깃에만 쓰고, 시점은 대체 Pod을 만들기 전이다 — 넘기기까지 미룰 이유가 없고, 그 사이 무관한 스케일다운이 나도 드레인 대상이 먼저 죽는 쪽이 낫다.

대체 Pod에는 쓰지 않는다. 입양 전에는 ReplicaSet이 쳐다보지 않는 Pod이라 값이 무의미하고, 입양 후에 남으면 그 Pod이 다음 스케일다운마다 1순위로 죽는다. 타깃은 지워지면서 값도 같이 사라지므로 걷을 것이 없다.

원래 값이 있었어도 덮어쓰고 복원하지 않는다. 되돌릴 때는 값이 정확히 `-2147483648`인 것만 지운다. 그 값을 쓰는 게 우리뿐이라 값이 이것이면 우리가 붙인 것이다.

### 3. 대체 Pod 맞추기

만들기만 하는 단계가 아니다. **있어야 할 집합과 있는 집합을 맞춘다.**

```
있어야 할 것 = deletionTimestamp 가 없는 타깃마다 하나
있는 것      = soft-drain.com/replaces = <타깃 UID> 이면서
               controller ownerRef 없고
               phase 가 Failed / Succeeded 가 아니고
               deletionTimestamp 도 없는 Pod

모자라면 만들고, 남으면 지운다
```

**타깃이 사라지면 대체 Pod도 사라진다.** 취소, 롤아웃으로 인한 ReplicaSet prune, Deployment 삭제, `replicas` 축소, 타깃의 eviction이 전부 이 한 줄에 걸린다. 따로 처리할 것이 없다.

**terminating 타깃은 만들기에서 뺀다.** ReplicaSet은 `deletionTimestamp`가 찍힌 Pod을 active에서 빼므로 이미 스스로 대체를 만들고 있고, 노드가 cordon이라 그 Pod은 다른 노드에 뜬다. 자리는 우리가 아무것도 안 해도 비워진다.

**죽은 대체 Pod은 있는 것으로 세지 않고, 지운다.** 노드 압력 eviction이나 kubelet admission 거부로 `Failed`가 된 Pod은 Ready가 될 수도 입양될 수도 없다. 살아 있는 것으로 세면 그 타깃이 영원히 멈춘다. Pod에는 재시작이 없어서(phase `Failed`는 터미널이고 `restartPolicy`는 컨테이너 얘기다) 복구는 새 Pod뿐인데, 세지 않고 지우지도 않으면 만들 때마다 시체가 쌓인다. 원인이 지속되면 만들고-죽고-지우기를 반복하다가 원인이 풀리는 순간 수렴한다.

**이미 넘긴 것도 세지 않는다.** 넘기면서 `soft-drain.com/replaces` 라벨을 떼기 때문에 애초에 후보가 아니다. 그래서 넘겼는데 타깃이 살아남은 경우가 자연히 복구된다 — 넘기는 순간 `replicas`가 올라가면 초과분이 증설분에 흡수되어 아무것도 안 지워지는데, 다음 라운드가 "타깃은 그대로인데 대신할 Pod이 없다"를 보고 하나 더 만든다.

**만드는 쪽과 지우는 쪽 양쪽에서 깨어나야 한다.** 노드에서 출발하는 순회만 있으면, ReplicaSet이 prune될 때 타깃 Pod도 같이 사라져서 순회할 대상이 없어지고 대체 Pod을 쳐다볼 일이 없어진다. 그래서 대체 Pod 자체를 키로 하는 경로가 따로 있어야 한다. 판정은 위 한 줄로 같다.

대체 Pod은 이렇게 만든다.

```yaml
metadata:
  generateName: aaa-5449d4d8c8-        # 타깃의 ReplicaSet 이름 + "-"
  labels:
    app: aaa                            # rs.spec.template.metadata.labels 에서
    soft-drain.com/replaces: 3f2a...     # 타깃 Pod의 UID
    # pod-template-hash 는 뺀다
spec: <rs.spec.template.spec 그대로>
```

스펙은 **살아 있는 Pod이 아니라 `rs.spec.template`에서** 가져온다. 살아 있는 Pod을 베끼면 `nodeName`이 따라오고, webhook이 이미 넣어둔 사이드카 위에 하나가 더 들어간다.

`rs.spec.template.metadata.labels`에는 `pod-template-hash`가 **이미 들어 있다.** 복사한 뒤 명시적으로 제거한다. 이 문서에서 가장 중요한 한 줄이다.

생성이 거부되면 Warning Event를 낸다. ResourceQuota 초과나 admission webhook 거부가 여기 걸리는데, 이 경우만 Pod 오브젝트가 안 생겨서 밖에서 볼 흔적이 없다. 거부돼도 멈추지 않고 다음 라운드에 다시 시도한다.

### 4. 넘기기

대체 Pod이 Ready가 되면 patch 하나로 `pod-template-hash`를 붙이고 `soft-drain.com/replaces`를 뗀다.

붙일 hash는 **타깃 Pod의 ownerRef가 가리키는 ReplicaSet**에서 읽는다. Deployment를 거쳐 현재 ReplicaSet을 찾는 경로는 쓰지 않는다 — 대체 Pod은 타깃의 ReplicaSet 템플릿으로 만들어졌고, 롤아웃 중이면 그게 현재 ReplicaSet이 아닐 수 있다.

넘기기 전에 두 가지를 본다.

**대체 Pod이 어느 노드에 앉았는가.** drain 라벨이 붙은 노드면 넘기지 않고 지운다. 넘겨봐야 그 노드에 타깃이 하나 더 생길 뿐이다. 아직 hash가 없어 어느 ReplicaSet의 자식도 아니므로 지워도 노출은 줄지 않고, 다음 라운드가 새로 만들면 스케줄러가 cordon된 노드를 피해 앉힌다. 지울 때 Warning Event를 남긴다. cordon보다 스케줄이 먼저여서 나중에 drain이 걸린 노드에 앉아 있던 경우가 이걸로 풀린다.

`node.kubernetes.io/unschedulable`을 tolerate하는 워크로드는 새로 만든 Pod이 또 drain 노드에 앉을 수 있고, 그러면 만들고 지우기를 반복한다. cordon을 무시하도록 만든 워크로드이므로 우리가 `nodeAffinity`를 주입해 그 의도를 뒤집지 않는다. 반복되는 Warning Event가 옮길 수 없다는 사실을 보여준다.

**사용자 Deployment가 Healthy한가.**

```
Healthy(D) ≡ D.status.observedGeneration >= D.metadata.generation
           ∧ D.status.replicas == D.status.updatedReplicas
           ∧ D.status.availableReplicas >= D.spec.replicas
```

Healthy가 아니면 미룬다. 사용자가 `N` 미만이면 넘겨도 초과분이 없어 아무것도 지워지지 않고, 롤아웃 중이면 넘겨받을 ReplicaSet이 하나로 정해지지 않아 노출이 rollout 설정보다 더 내려갈 수 있다.

`replicas == updatedReplicas` 항이 "Pod을 가진 ReplicaSet이 하나뿐"을 판정한다. 나머지 두 항만으로는 롤아웃을 못 잡는다 — `maxUnavailable: 0`이면 `availableReplicas >= N`이 롤아웃 내내 유지되는데, 무중단을 원하는 사용자가 정확히 그 설정을 쓴다. `spec.paused`도 이 항에 걸린다.

판정은 Deployment마다 따로 하고 준비된 것부터 넘긴다. 묶으면 제일 느린 하나가 나머지를 인질로 잡는다.

### 5. 완료

노드 위에 타깃이 하나도 없으면 `cordoned-by-controller` 어노테이션을 지우고 `state=Complete`를 붙인 뒤 Event를 낸다. 어노테이션을 지우는 것은 cordon의 소유권을 사람에게 넘긴다는 뜻이다. `Complete`는 래치다 — cordon이 유지되는 동안에는 drain 라벨이 걷힐 때까지 관여하지 않는다. cordon된 노드에 새로 앉을 수 있는 건 `unschedulable`을 tolerate하는 Pod뿐인데, 그건 어차피 옮기지 못하는 부류다. 이게 없으면 완료를 확인하고 라벨을 정리한 사용자가 노드를 다시 열게 되고, 비워 둔 노드에 Pod이 몰린 채로 리부팅하게 된다.

소유권을 넘겨받은 사람이 uncordon하면 래치는 `Cancelled`로 접힌다(1번). 착지 금지도 함께 풀린다 — 리부팅하러 갈 노드라서 막았던 것인데, uncordon은 리부팅 안 간다는 선언이다. uncordon과 감지 사이의 짧은 창에서는 착지한 대체 Pod이 지워질 수 있지만, 다음 라운드가 새로 만든다.

**완료 판정에는 terminating 타깃도 센다.** `deletionTimestamp`가 찍혀도 grace period 동안 계속 돈다. 여기서 빼면 아직 작업이 돌고 있는 노드에 `Complete`가 붙고, 그걸 보고 노드를 리부팅한 사람이 그 작업을 죽인다. 만들기에서는 빼고 완료 판정에서는 세는 이유가 이것이다.

## 메타데이터

| 대상 | 키 | 값 | 쓰는 쪽 |
|---|---|---|---|
| 노드 | `soft-drain.com/drain` (라벨) | `"true"` | 사람 |
| 노드 | `soft-drain.com/state` (라벨) | `InProgress` / `Complete` / `Cancelled` | 컨트롤러 |
| 노드 | `soft-drain.com/cordoned-by-controller` (어노테이션) | `"true"` | 컨트롤러 |
| 타깃 Pod | `controller.kubernetes.io/pod-deletion-cost` (어노테이션) | `-2147483648` | 컨트롤러 |
| 대체 Pod | `soft-drain.com/replaces` (라벨) | 타깃 Pod의 UID | 컨트롤러 |

`soft-drain.com/replaces`를 쓰는 것은 우리뿐이다. 이 라벨이 없는 Pod은 만들지도 지우지도 않는다.

## 지켜야 할 것

1. 대체 Pod은 `pod-template-hash` 없이 만든다.
2. 대체 Pod의 스펙은 살아 있는 Pod이 아니라 `rs.spec.template`에서 가져온다.
3. `pod-deletion-cost`를 먼저 쓰고 `pod-template-hash`를 나중에 붙인다.
4. `soft-drain.com/replaces` 라벨이 있는 Pod만 지운다.
5. controller ownerRef가 있는 Pod은 지우지 않는다.
6. 대체 Pod을 지울 때는 읽었던 UID와 resourceVersion을 preconditions로 건다. 판정과 삭제 사이에 hash가 붙어 ReplicaSet이 데려간 Pod이면 삭제가 거부되고, 다음 라운드가 다시 판정한다.

## 안 하는 것

- Deployment 소속 Pod만 옮긴다. StatefulSet, DaemonSet, Job, 직접 만든 Pod은 그대로 둔다. **`Complete`는 "내 몫이 끝났다"이지 "노드가 비었다"가 아니다.**
- 노드 위 대상 Pod을 한꺼번에 옮긴다. 자원이 모자라면 Pending으로 기다린다.
- 옮길 수 없는 워크로드를 미리 걸러내지 않는다. Pending으로 남고 사람이 보면 된다.
- PDB를 조회하지 않는다. 지우는 주체가 사용자 ReplicaSet이라 eviction API를 타지 않는다.
- 사용자 Deployment의 `spec`을 수정하지 않는다. 사용자 Pod에는 어노테이션 하나만 쓴다.
- 노드를 drain하거나 끄지 않는다.

## 막혔을 때 보는 법

노드가 `InProgress`에서 안 움직이면 대체 Pod을 본다.

```bash
kubectl get pods -A -l soft-drain.com/replaces
kubectl describe pod <Pending 인 것>
```

스케줄러가 `PodScheduled=False`의 message에 이유를 그대로 써 둔다 — `0/12 nodes are available: 5 Insufficient cpu, 7 node(s) didn't match pod anti-affinity rules` 같은 식이다. 컨트롤러가 따로 진단을 만들지 않는 이유다.

대체 Pod이 하나도 안 보이면 생성 자체가 거부된 것이다. `kubectl describe node <노드>`로 Event를 본다.

## 알려진 한계

- **여유 자원이 없으면 진행하지 못한다.** 자리는 옛 Pod이 죽어야 나고 옛 Pod은 새 Pod이 Ready여야 죽으므로, 여유가 0이면 스스로 풀리지 않는다. RWO 볼륨과 로컬 PV도 같은 구조다.
- **배치 규칙이 한 자리를 못 내주면 자원이 남아돌아도 진행하지 못한다.** 노드당 하나로 제한하는 required `podAntiAffinity`를 걸어두고 후보 노드를 전부 채운 경우가 대표적이다. 이건 soft-drain만의 제약이 아니라 "먼저 띄우고 나중에 지운다"는 방식 전체의 산술이다 — 같은 워크로드는 `maxSurge: 1` 롤아웃도 똑같이 막힌다. 그래서 그런 사용자는 이미 `maxUnavailable: 1`로 운영하며 롤아웃마다 `N` 밑으로 내려가는 것을 감수하고 있다. 노드를 뺄 때도 `kubectl drain`을 쓰면 된다.
- **`node.kubernetes.io/unschedulable`을 tolerate하는 워크로드는 옮기지 못할 수 있다.** 대체 Pod이 drain 노드에 앉을 때마다 지우고 다시 만들기를 반복하고, 다른 노드에 앉는 운이 따라야 끝난다.
- **입양 전 대체 Pod은 controller가 없어 PDB 집계를 흔든다.** 같은 라벨을 갖고 Ready라 `currentHealthy`에는 들어가는데 `expectedCount`에는 안 들어가서, 그동안 `disruptionsAllowed`가 1 늘어난다. PDB가 지키는 바닥 아래로 내려가지는 않는다. 같은 이유로 사용자 PDB에 `UnmanagedPods` Warning이 쌓인다.
- **주인 없는 대체 Pod이 있는 노드는 Cluster Autoscaler가 축소하지 못한다.** 넘기기가 멈춘 상태로 오래 가면 그 노드가 컨솔리데이션에서 계속 빠진다.
- **롤아웃과 겹치면 곧 버려질 대체 Pod을 만든다.** 롤아웃이 끝나면 타깃이 사라지면서 같이 지워지지만, 그동안 롤아웃의 `maxSurge`와 자리를 다툰다.
- **사용자 Pod의 `pod-deletion-cost` 원래 값은 복원하지 않는다.**
- **`pod-deletion-cost`가 필요하므로 Kubernetes 1.22 이상이어야 한다.**
