#ifndef SHMEM_TEAMS_H_INCLUDED
#define SHMEM_TEAMS_H_INCLUDED

#include "dart_mempool.h"
#include "shmem_group.h"
#include "dart/dart_team.h"

#define TEAM_STATE_NOTINIT  1
#define TEAM_STATE_INIT     2

struct team_impl_struct {
	int id;    // the team id
	int state; // initialized or not

	// the members of the team
	dart_group_t group;
	// associated mempool (may be DART_MEMPOOL_NULL)
	dart_mempool mempool;
	int barrier_idx;
};

int dart_team_l2g(int teamid, int id);
int dart_team_g2l(int teamid, int id);

dart_mempool dart_team_mempool(int teamid);

#endif /* SHMEM_TEAMS_H_INCLUDED */
