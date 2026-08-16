# Changelog

## 1.0.0 (2026-08-16)


### Features

* kubectl 플러그인 완성 — status·release 서브커맨드와 현황판 ([e4daf49](https://github.com/rogeeoh/soft-drain/commit/e4daf490e3722577a0f61e9af063f3413d9f8864))
* kubectl 플러그인, 라벨 프리픽스 soft-drain.com, 영문 README ([acdc652](https://github.com/rogeeoh/soft-drain/commit/acdc65240d7418ab67e0901b6d04fbd1eef69983))
* MVP 마감 — version 서브커맨드, 자동화 계약·최소 RBAC 문서 ([47935c0](https://github.com/rogeeoh/soft-drain/commit/47935c0076360fe493db71e3a74f7415457f2206))
* release-please와 ghcr 릴리스 파이프라인, Helm 차트 ([80e1e4a](https://github.com/rogeeoh/soft-drain/commit/80e1e4ab7f597d3dbbcb59f9a66e0db3ade0b1eb))
* uncordon은 상태와 무관하게 관여 종료 — Complete 뒤에도 접는다 ([f01ffc8](https://github.com/rogeeoh/soft-drain/commit/f01ffc8e857263877a00eff2002a8b180b282e79))
* 릴리스에 kubectl 플러그인 바이너리 첨부 ([e9f5c26](https://github.com/rogeeoh/soft-drain/commit/e9f5c2694c2f9b5f651ab7e66b8f8233bcc4ac40))
* 배치·회수 컨트롤러 구현과 리뷰 1라운드 반영 ([f5219ab](https://github.com/rogeeoh/soft-drain/commit/f5219ab226fc4aa6e427ce80ca6361bdde01ccad))
* 플러그인이 노드 여러 개를 받는다 — kubectl drain처럼 ([3f03da6](https://github.com/rogeeoh/soft-drain/commit/3f03da61a8fcae3ba42bc5939485e595dbfd77d2))


### Bug Fixes

* Ctrl-C는 실패가 아니라 분리 — error 없이 안내만, 종료코드 130 ([87c1dce](https://github.com/rogeeoh/soft-drain/commit/87c1dce288c41ff6cab448c941be59acf8781ffe))
* 완성 스크립트가 첫 인자를 건너뛰던 오프바이원 — kubectl은 플러그인 이름 뒤 인자만 넘긴다 ([ecba93b](https://github.com/rogeeoh/soft-drain/commit/ecba93bd3fb9ad8cf733f6fed009f5ccf81185fa))
