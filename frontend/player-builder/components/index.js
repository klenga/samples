import React, { useState } from 'react'
import {
  episodesLoadingSelector,
  episodesSelector,
  networksLoadingSelector,
  networksSelector
} from 'redux/modules/media'
import { userPreferenceSelector, userSeriesLoadingSelector, userSeriesSelector } from 'redux/modules/profiles'

import Breadcrumb from 'components/Breadcrumb'
import EmbedCodeForm from 'components/EmbedCodeForm'
import LoadingIndicator from 'components/LoadingIndicator'
import PropTypes from 'prop-types'
import Tabs from 'components/Tabs'
import ThumbnailPodcastsMenu from 'components/ThumbnailPodcastsMenu'
import { compose } from 'redux'
import { connect } from 'react-redux'
import { createStructuredSelector } from 'reselect'
import fp from 'lodash/fp'
import { makeStyles } from '@material-ui/core/styles'
import styles from './styles'

const useStyles = makeStyles(styles)

const tabs = [{ label: 'Series Player', key: 'series' }, { label: 'Episode Player', key: 'episode' }]

const renderTab = (tab, episode, podcast, network, classes) => {
  switch (tab) {
    case 'series':
      return (
        <div className={classes.tabContent}>
          <EmbedCodeForm podcast={podcast} disablePreview={false} />
        </div>
      )
    case 'episode':
      return (
        <div className={classes.tabContent}>
          <Breadcrumb network={network} podcast={podcast} episode={episode} />
          <EmbedCodeForm podcast={podcast} episode={episode} disablePreview={episode == null} />
        </div>
      )
    default:
      return ''
  }
}

const renderNetwork = (network, classes) => (
  <div className={classes.root}>
    <div className={classes.content}>
      <ThumbnailPodcastsMenu networkId={network.networkId} podcastId={null} />
      Select a podcast from the list above.
    </div>
  </div>
)

const renderPodcast = (podcast, network, classes, activeTab, setActiveTab) => {
  const tabContent = renderTab(activeTab, null, podcast, network, classes)

  return (
    <div className={classes.root}>
      <div className={classes.content}>
        <ThumbnailPodcastsMenu networkId={network.networkId} podcastId={podcast.seriesId} />
        <div className={classes.podcastTitle}>{podcast.name}</div>
        <Tabs tabs={tabs} activeKey={activeTab} onChange={setActiveTab} />
        {tabContent}
      </div>
    </div>
  )
}

const renderEpisode = (episode, podcast, network, classes, activeTab, setActiveTab) => {
  const tabContent = renderTab(activeTab, episode, podcast, network, classes)
  return (
    <div className={classes.root}>
      <div className={classes.content}>
        <ThumbnailPodcastsMenu networkId={network.networkId} podcastId={podcast.seriesId} />
        <div className={classes.podcastTitle}>{podcast.name}</div>
        <Tabs tabs={tabs} activeKey={activeTab} onChange={setActiveTab} />
        {tabContent}
      </div>
    </div>
  )
}

const renderFallback = classes => (
  <div className={classes.root}>
    Use the search box above to find a podcast, or choose a network in the upper right corner.
  </div>
)

const renderLoading = classes => (
  <div className={classes.root}>
    <LoadingIndicator />
  </div>
)

const PlayerBuilderMain = ({
  episode,
  network,
  podcast,
  episodesLoading,
  networksLoading,
  userSeriesLoading,
  userPreference
}) => {
  const classes = useStyles()
  const [activeTab, setActiveTab] = useState(episode ? tabs[1].key : tabs[0].key)

  const isLoading = episodesLoading || networksLoading || userSeriesLoading
  if (isLoading) {
    return renderLoading(classes)
  } else if (episode) {
    return renderEpisode(episode, podcast, network, classes, activeTab, setActiveTab)
  } else if (podcast) {
    return renderPodcast(podcast, network, classes, activeTab, setActiveTab)
  } else if (network) {
    return renderNetwork(network, classes)
  } else {
    return renderFallback(classes)
  }
}

PlayerBuilderMain.propTypes = {
  episode: PropTypes.object,
  episodesLoading: PropTypes.bool,
  network: PropTypes.object,
  networksLoading: PropTypes.bool,
  podcast: PropTypes.object,
  userPreference: PropTypes.object,
  userSeriesLoading: PropTypes.bool
}

const networkSelector = (state, { userPreference }) =>
  compose(
    fp.find({ networkId: fp.get('networkId')(userPreference) }),
    fp.defaultTo([]),
    networksSelector
  )(state)

const podcastSelector = (state, { userPreference }) =>
  compose(
    fp.find({ seriesId: fp.get('seriesId')(userPreference) }),
    fp.defaultTo([]),
    fp.get(fp.get('networkId')(userPreference)),
    userSeriesSelector
  )(state)

const episodeSelector = (state, { userPreference }) =>
  compose(
    fp.find({ episodeId: fp.get('episodeId')(userPreference) }),
    fp.defaultTo([]),
    episodesSelector
  )(state)

const selector1 = createStructuredSelector({
  userPreference: userPreferenceSelector
})

const selector2 = createStructuredSelector({
  episode: episodeSelector,
  episodesLoading: episodesLoadingSelector,
  network: networkSelector,
  networksLoading: networksLoadingSelector,
  podcast: podcastSelector,
  userSeriesLoading: userSeriesLoadingSelector
})

export default compose(
  connect(selector1),
  connect(selector2)
)(PlayerBuilderMain)
